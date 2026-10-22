"""Long run: one simulated week with a warm-up, thousands of entities and telemetry
records, low-level API.

The low-level twin of `builder/long_run_example.py`, a regression scenario (see
scripts/regression.py), not an example. Orders arrive all week, with a busy spell from
day 3 to day 4. The first 6 hours are warm-up, which no sink receives; the statistics
at the `all` level are published every hour.
"""

import itertools

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
    run_scenario,
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.01)},
    service={
        "packing": DistributionConfig(dist="gamma", shape=4, scale=25),
        "shipping": DistributionConfig(dist="lognormal", mean=60, std=30),
    },
    resources={"packers": CapacityConfig(current_cap=3, max_cap=4)},
)
WARMUP = 21600.0  # seconds
SCHEDULE = [
    {
        "at": 172800,
        "path": "Shop.arrival.orders.rate",
        "value": 0.015,
        "record": {"fault": "busy_period"},
    },
    {
        "at": 259200,
        "path": "Shop.arrival.orders.rate",
        "value": 0.01,
        "record": {"fault": "busy_period_over"},
    },
]


def order(env, sampler, shop, key, task_id):
    """Packs an order, then ships it."""

    def publish(task, instance, value):
        env.publish_event(key, value, task=task, instance=instance)

    pack_path, ship_path = "Shop.service.packing", "Shop.service.shipping"
    env.stats.flow_started("orders")
    publish("pack", task_id, {"path_id": pack_path, "status": "queued"})
    env.stats.task_queued("pack", task_id)
    packers = shop["packers"]
    request = packers.request()
    yield request
    publish("pack", task_id, {"path_id": pack_path, "status": "started"})
    env.stats.task_started("pack", task_id)
    yield env.timeout(sampler.sample(env.registry.get_config(pack_path)))
    publish("pack", task_id, {"event_type": "order_packed"})
    env.stats.task_finished("pack", task_id)
    env.notify_change()
    packers.release(request)
    ship_id = next(shop["ship_ids"])
    publish("ship", ship_id, {"path_id": ship_path, "status": "started"})
    env.stats.task_started("ship", ship_id)
    yield env.timeout(sampler.sample(env.registry.get_config(ship_path)))
    publish("ship", ship_id, {"event_type": "order_shipped"})
    env.stats.task_finished("ship", ship_id)
    env.notify_change()
    env.stats.flow_finished("orders")


def order_generator(env, sampler, shop):
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(order(env, sampler, shop, f"orders-{number}", number - 1))


def build(env):
    """The packers, and the counter of ship instances, which finish in their own order."""
    return {
        "packers": DynamicResource(env, "Shop", "packers"),
        "ship_ids": itertools.count(),
    }


def processes(env, sampler, shop):
    """The arrivals and the schedule, in the builder's order."""
    return [
        order_generator(env, sampler, shop),
        run_scenario(env, SCHEDULE, until=604800),
    ]


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()], warmup=WARMUP)
    env.setup_stats("Shop", "all", interval=3600.0)
    sampler = Sampler(seed=42)
    shop = build(env)
    for process in processes(env, sampler, shop):
        env.process(process)
    try:
        env.run(until=604800)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
