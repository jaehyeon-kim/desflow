"""The Hand Off to a Resource guide, low-level API.

The low-level version of multi_resource_handoffs.py: pick requests the picker and
passes the granted request to pack, which releases it once the order is on the bench.
python multi_resource_handoffs_low_level.py runs 30 seconds in real time.
"""

import itertools
import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

logging.basicConfig(level=logging.INFO)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=1.0)},
    service={
        # The picker takes 1 s to carry an order to the bench, and packing takes 4 s
        "walking": DistributionConfig(dist="constant", value=1.0),
        "packing": DistributionConfig(dist="constant", value=4.0),
    },
    resources={
        "picker": CapacityConfig(current_cap=1, max_cap=1),
        "bench": CapacityConfig(current_cap=1, max_cap=1),
    },
)


# --8<-- [start:tasks]
def pick(env, shop, key):
    """Carries the order to the bench, and hands the picker to pack."""
    task_id, path_id = next(shop["ids"]["pick"]), "Shop.service.walking"
    env.publish_event(key, {"path_id": path_id, "status": "queued"}, "pick", task_id)
    picker = shop["picker"].request()
    yield picker
    env.publish_event(key, {"path_id": path_id, "status": "started"}, "pick", task_id)
    yield env.timeout(shop["sampler"].sample(env.registry.get_config(path_id)))
    env.publish_event(key, {"event_type": "order_picked"}, "pick", task_id)
    env.process(pack(env, shop, key, picker))  # the order still holds the picker


def pack(env, shop, key, picker):
    """Waits for the bench while holding the picker, then gives the picker back."""
    task_id, path_id = next(shop["ids"]["pack"]), "Shop.service.packing"
    env.publish_event(key, {"path_id": path_id, "status": "queued"}, "pack", task_id)
    with shop["bench"].request() as bench:
        yield bench
        env.publish_event(
            key, {"path_id": path_id, "status": "started"}, "pack", task_id
        )
        shop["picker"].release(picker)  # the order is on the bench
        yield env.timeout(shop["sampler"].sample(env.registry.get_config(path_id)))
        env.publish_event(key, {"event_type": "order_packed"}, "pack", task_id)


# --8<-- [end:tasks]
def orders(env, shop):
    """Starts pick for every order, keyed orders-1, orders-2 and so on."""
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(shop["sampler"].sample(gap))
        env.process(pick(env, shop, f"orders-{number}"))


def run(until=30.0, factor=1.0, egress=None):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress(egress or [ConsoleEgress()])
    shop = {
        "picker": DynamicResource(env, "Shop", "picker"),
        "bench": DynamicResource(env, "Shop", "bench"),
        "sampler": Sampler(seed=42),
        "ids": {"pick": itertools.count(), "pack": itertools.count()},
    }
    env.process(orders(env, shop))
    try:
        env.run(until=until)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
