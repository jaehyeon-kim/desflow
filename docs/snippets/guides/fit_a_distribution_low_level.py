"""The Fit a Distribution guide, low-level API: the shop with arrivals fitted to an
order log.

python fit_a_distribution_low_level.py fits the gaps between the orders in
order_log.csv, as fit_a_distribution.py does, and runs the shop for an hour with the
fitted arrivals. It prints the number of orders packed.
"""

import asyncio
import itertools
import queue

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)
from fit_a_distribution import fit


class Count:
    """An egress that counts the orders packed."""

    def __init__(self):
        self.packed = 0

    async def run(self, egress_queue):
        try:
            while True:
                try:
                    for record in egress_queue.get_nowait():
                        value = record["value"]
                        self.packed += value.get("event_type") == "order_packed"
                except queue.Empty:
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass


# --8<-- [start:load]
def params(result) -> SimParameter:
    """One packer, with the fitted arrivals."""
    return SimParameter(
        sim_id="Shop",
        # The best fit's settings, such as dist="exponential", rate=0.4
        arrival={"orders": DistributionConfig(**result.settings())},
        service={"packing": DistributionConfig(dist="gamma", shape=4, scale=0.375)},
        resources={"packers": CapacityConfig(current_cap=1, max_cap=2)},
    )


# --8<-- [end:load]


def pack_order(env, packers, sampler, key, task_id):
    path_id = "Shop.service.packing"
    env.publish_event(
        key, {"path_id": path_id, "status": "queued"}, "pack_order", task_id
    )
    with packers.request() as unit:
        yield unit
        env.publish_event(
            key, {"path_id": path_id, "status": "started"}, "pack_order", task_id
        )
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        env.publish_event(key, {"event_type": "order_packed"}, "pack_order", task_id)


def orders(env, packers, sampler):
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


def run(result, until: float = 3600) -> int:
    """Runs the shop for an hour and returns the number of orders packed."""
    count = Count()
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(params(result))
    env.setup_egress([count])
    packers = DynamicResource(env, "Shop", "packers")
    env.process(orders(env, packers, Sampler(seed=42)))
    env.run(until=until)
    env.teardown()  # waits until the sink has taken every batch
    return count.packed


if __name__ == "__main__":
    print(f"orders packed in an hour: {run(fit())}")
