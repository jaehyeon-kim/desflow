"""The Switch a Task On Mid-run guide, low-level API: from 1 minute, one order in four
is gift wrapped after packing.

The low-level version of switch_a_task_on_builder.py: a Router reads the weight from
the registry each time an order is packed, and `run_scenario` changes it at 1 minute.
python switch_a_task_on_low_level.py runs 5 simulated minutes at once.
"""

import itertools
import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Router,
    Sampler,
    SimParameter,
    Upstream,
    run_scenario,
)

logging.basicConfig(level=logging.INFO)

UNTIL = 300
# --8<-- [start:declare]
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="constant", value=5.0)},
    service={
        "packing": DistributionConfig(dist="constant", value=2.0),
        "wrapping": DistributionConfig(dist="constant", value=3.0),
    },
    resources={
        "packers": CapacityConfig(current_cap=1, max_cap=1),
        # The new task's resource exists from the start, unused until the switch
        "wrappers": CapacityConfig(current_cap=1, max_cap=1),
    },
    # The switch: a weight of 0 sends no order to wrap_gift
    variables={"wrap_weight": 0},
)
# --8<-- [end:declare]
# --8<-- [start:route]
# Read when each order is packed: 3 to 0 before the switch, 3 to 1 after it
ROUTE = {
    "weights": {"ship_order": 3, "wrap_gift": 'registry["Shop.variables.wrap_weight"]'}
}
# --8<-- [end:route]
# --8<-- [start:scenario]
# At 1 minute wrapping starts, and the step records what it did
SCHEDULE = [
    {
        "at": 60,
        "path": "Shop.variables.wrap_weight",
        "value": 1,
        "record": {"intervention": "gift_wrapping_on"},
    }
]
# --8<-- [end:scenario]


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the orders and the scenario on an environment that has PARAMS."""
    sampler = Sampler(seed=42)
    route = Router(ROUTE)
    # The routing task's own stream, as the blueprint and the builder draw from
    route.rng = sampler.generator("route.pack_order")
    shop = {
        "packers": DynamicResource(env, "Shop", "packers"),
        "wrappers": DynamicResource(env, "Shop", "wrappers"),
    }
    ids = {
        name: itertools.count() for name in ("pack_order", "wrap_gift", "ship_order")
    }

    def serve(key, task, service, resource, payload):
        task_id = next(ids[task])
        env.publish_event(key, {"status": "queued"}, task, task_id)
        with shop[resource].request() as request:
            yield request
            env.publish_event(key, {"status": "started"}, task, task_id)
            path = f"Shop.service.{service}"
            yield env.timeout(sampler.sample(env.registry.get_config(path)))
            env.publish_event(key, payload, task, task_id)
        return task_id

    def order(key):
        task_id = yield from serve(
            key, "pack_order", "packing", "packers", {"event_type": "order_packed"}
        )
        packed = Upstream(task_id, key, {}, {"event_type": "order_packed"})
        if route.choose(packed, env=env) == "wrap_gift":
            yield from serve(
                key, "wrap_gift", "wrapping", "wrappers", {"event_type": "gift_wrapped"}
            )
        env.publish_event(
            key, {"event_type": "order_shipped"}, "ship_order", next(ids["ship_order"])
        )

    def orders():
        gap = env.registry.get_config("Shop.arrival.orders")
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(order(f"orders-{number}"))

    env.process(orders())
    env.process(run_scenario(env, SCHEDULE, until=UNTIL))


def run(egress=None):
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress(egress or [ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
