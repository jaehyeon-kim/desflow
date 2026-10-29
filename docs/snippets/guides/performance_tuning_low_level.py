"""The Tune Performance guide, low-level API: records per second of a model, without
its sinks.

python performance_tuning_low_level.py prints the records a one-hour run of the shop
below published, and how many a second.
"""

import gc
import itertools
import time

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)
from performance_tuning import Count

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=10.0)},
    service={"packing": DistributionConfig(dist="normal", mean=2.0, std=0.4)},
    resources={"packers": CapacityConfig(current_cap=25, max_cap=25)},
)


# --8<-- [start:shop]
def pack_order(env, packers, sampler, key, task_id):
    path_id, stats = "Shop.service.packing", env.stats
    env.publish_event(
        key, {"path_id": path_id, "status": "queued"}, "pack_order", task_id
    )
    stats.task_queued("pack_order", task_id)
    with packers.request() as unit:
        yield unit
        env.publish_event(
            key, {"path_id": path_id, "status": "started"}, "pack_order", task_id
        )
        stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        env.publish_event(key, {"event_type": "order_packed"}, "pack_order", task_id)
        stats.task_finished("pack_order", task_id)


def orders(env, packers, sampler):
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


def shop(egress: list) -> DynamicRealtimeEnvironment:
    """Ten orders a second and 25 packers, publishing to the sinks given."""
    env = DynamicRealtimeEnvironment(factor=0)  # no wall clock to wait for
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress(egress)
    # minimal: readings every 10 s, not the costlier task statistics
    env.setup_stats("Shop", "minimal", interval=10)
    packers = DynamicResource(env, "Shop", "packers")
    env.process(orders(env, packers, Sampler(seed=42)))
    return env


# --8<-- [end:shop]


# --8<-- [start:measure]
def throughput(until: float) -> dict:
    """Runs the shop with a counting sink: the record count and the records per second."""
    count = Count()
    env = shop([count])
    gc.collect()
    gc.freeze()  # leave the objects built so far out of garbage collection
    started = time.perf_counter()
    env.run(until=until)
    env.teardown()  # waits until the sink has taken every batch
    seconds = time.perf_counter() - started
    gc.unfreeze()
    return {"records": count.records, "per_second": round(count.records / seconds)}


# --8<-- [end:measure]

if __name__ == "__main__":
    print(throughput(until=3600))
