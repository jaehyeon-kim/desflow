"""The A Flow of Tasks tutorial: orders are picked, packed and shipped, low-level.

Two pickers, two packers and a shelf of boxes restocked every 100 seconds. An order
picked while the shelf is empty goes to backorder for a minute and is picked again,
and one parcel in 20 is packed again. Every record is printed to the terminal.
"""

import contextlib
import itertools
import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    ContainerConfig,
    DistributionConfig,
    DynamicContainer,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Entity,
    Router,
    Sampler,
    SimParameter,
    Upstream,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={
        "orders": DistributionConfig(dist="exponential", rate=0.4),
        "express": DistributionConfig(dist="exponential", rate=0.1),
        "deliveries": DistributionConfig(dist="constant", value=100),
    },
    service={
        "picking": DistributionConfig(dist="uniform", low=1, high=3),
        "packing": DistributionConfig(dist="constant", value=1.5),
        # The supplier takes a minute to send an item that is out of stock
        "supplying": DistributionConfig(dist="constant", value=60),
    },
    resources={
        "pickers": CapacityConfig(current_cap=2, max_cap=4),
        "packers": CapacityConfig(current_cap=2, max_cap=4),
    },
    # Boxes on the shelf, restocked with 53 every 100 seconds
    containers={"stock": ContainerConfig(current_cap=200, max_cap=200, level=50)},
)
# --8<-- [start:attributes]
# Each order starts with the attributes given here, read as attr.<name>
ATTRIBUTES = {"orders": {"express": 0}, "express": {"express": 1}}
# --8<-- [end:attributes]

# Each task's service and resource; None for a timed step or a task that takes no time
TASKS = {
    "pick_order": ("picking", "pickers"),
    "backorder": ("supplying", None),
    "pack_order": ("packing", "packers"),
    "ship_order": (None, None),
}
EVENTS = {
    "pick_order": "order_picked",
    "backorder": "order_backordered",
    "pack_order": "order_packed",
}
ROUTES = {
    # --8<-- [start:chain]
    # The first rule that holds wins; default catches the rest
    "pick_order": Router(
        {
            "when": [{"condition": "level.stock == 0", "task": "backorder"}],
            "default": "pack_order",
        }
    ),
    # --8<-- [end:chain]
    # --8<-- [start:backorder]
    "backorder": Router("pick_order"),
    # --8<-- [end:backorder]
    # --8<-- [start:rework]
    # One parcel in 20 fails its check and is packed again; the generator is set
    # in run below
    "pack_order": Router({"weights": {"ship_order": 95, "pack_order": 5}}),
    # --8<-- [end:rework]
    "ship_order": Router(None),
}
# --8<-- [start:set]
# Applied to the order's attributes when the step finishes
SETS = {"backorder": {"backordered": 1}}
# --8<-- [end:set]


def payload(name, entity):
    if name == "ship_order":
        # A payload includes attributes only when it asks for them
        return {
            "event_type": "order_shipped",
            "express": entity.attributes.get("express"),
            "backordered": entity.attributes.get("backordered"),
        }
    return {"event_type": EVENTS[name]}


def task(env, sampler, shop, name, entity, task_id, counters):
    """Runs one task for an order, then starts the task its router chooses."""
    service, resource = TASKS[name]
    path_id = f"Shop.service.{service}"

    def publish(value):
        env.publish_event(entity.key, value, task=name, instance=task_id)

    if resource is not None:
        publish({"path_id": path_id, "status": "queued"})
        env.stats.task_queued(name, task_id)
    unit = shop[resource].request() if resource else contextlib.nullcontext()
    with unit as request:
        if request is not None:
            yield request
        if name == "pack_order":
            yield shop["stock"].get(1)
        if service is not None:
            publish({"path_id": path_id, "status": "started"})
            env.stats.task_started(name, task_id)
        finished = payload(name, entity)
        if service is not None:
            yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish(finished)
        env.stats.task_finished(name, task_id)

    # --8<-- [start:next]
    entity.attributes.update(SETS.get(name, {}))
    upstream = Upstream(task_id, entity.key, entity.attributes, finished)
    following = ROUTES[name].choose(upstream, env, levels={"stock": shop["stock"]})
    if following is None:
        env.stats.flow_finished(entity.arrival)  # the order leaves the shop
        return
    instance = next(counters[following])
    env.process(task(env, sampler, shop, following, entity, instance, counters))
    # --8<-- [end:next]


def order_generator(env, sampler, shop, arrival, counters):
    """Starts an order for every arrival, keyed orders-1, express-1 and so on."""
    gap = env.registry.get_config(f"Shop.arrival.{arrival}")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        entity = Entity(f"{arrival}-{number}", arrival, dict(ATTRIBUTES[arrival]))
        instance = next(counters["pick_order"])
        env.process(task(env, sampler, shop, "pick_order", entity, instance, counters))


def delivery_generator(env, sampler, shop):
    gap = env.registry.get_config("Shop.arrival.deliveries")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        yield shop["stock"].put(53)
        value = {"event_type": "stock_delivered"}
        env.publish_event(
            f"deliveries-{number}", value, task="restock", instance=number - 1
        )
        env.stats.task_finished("restock", number - 1)
        env.stats.flow_finished("deliveries")


def run(until=3600, factor=0.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # Finished counts of every task and flow, at the end
    env.setup_stats("Shop", "basic", interval=3600)
    shop = {
        "pickers": DynamicResource(env, "Shop", "pickers"),
        "packers": DynamicResource(env, "Shop", "packers"),
        "stock": DynamicContainer(env, "Shop", "stock"),
    }
    sampler = Sampler(seed=42)
    # Weights draw from the routing task's own sequence, as the builder's do
    ROUTES["pack_order"].rng = sampler.generator("route.pack_order")
    # One counter per task, shared by the arrivals and the rules
    counters = {name: itertools.count() for name in TASKS}
    for arrival in ("orders", "express"):
        env.process(order_generator(env, sampler, shop, arrival, counters))
    env.process(delivery_generator(env, sampler, shop))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


if __name__ == "__main__":
    run()
