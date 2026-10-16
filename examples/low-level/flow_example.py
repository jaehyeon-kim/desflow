"""A flow of tasks, low-level API: pick, wrap gifts, pack, then ship.

The low-level twin of `builder/flow_example.py`. Each order is an `Entity` whose key,
such as `orders-1`, every event carries, with the task's name and instance id. A
`Router` per task takes the same rules as the builder's `next` and returns the name
of the task to start: gift orders are wrapped after picking, and one parcel in ten
is packed again. Wrapping and shipping are timed steps, which use no resource.

Needs no containers. Runs unpaced and ends after 120 simulation seconds.
"""

import contextlib
import itertools
import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
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
        "gifts": DistributionConfig(dist="exponential", rate=0.1),
    },
    service={
        "picking": DistributionConfig(dist="gamma", shape=2, scale=1),
        "wrapping": DistributionConfig(dist="constant", value=3),
        "packing": DistributionConfig(dist="lognormal", mean=2.5, std=1),
        "shipping": DistributionConfig(
            dist="empirical", values=[5, 10, 30], weights=[6, 3, 1]
        ),
    },
    resources={
        "pickers": CapacityConfig(current_cap=2, max_cap=4),
        "packers": CapacityConfig(current_cap=2, max_cap=4),
    },
)
ATTRIBUTES = {"orders": {"gift": 0}, "gifts": {"gift": 1}}

# Each task's service, its resource (None for a timed step), and what follows it
TASKS = {
    "pick": ("picking", "pickers"),
    "wrap": ("wrapping", None),
    "pack": ("packing", "packers"),
    "ship": ("shipping", None),
}
# --8<-- [start:routes]
ROUTES = {
    "pick": Router(
        {"when": [{"condition": "attr.gift == 1", "task": "wrap"}], "default": "pack"}
    ),
    "wrap": Router("pack"),
    # Weights draw from a generator, set in processes()
    "pack": Router({"weights": {"ship": 9, "pack": 1}}),
    "ship": Router(None),
}
SETS = {"wrap": {"wrapped": 1}}
# --8<-- [end:routes]


def payload(name, entity):
    """Each task's finished payload; ship asks for two attributes."""
    if name == "ship":
        return {
            "event_type": "order_shipped",
            "gift": entity.attributes.get("gift"),
            "wrapped": entity.attributes.get("wrapped"),
        }
    events = {"pick": "order_picked", "wrap": "gift_wrapped", "pack": "order_packed"}
    return {"event_type": events[name]}


def task(env, sampler, shop, name, entity, task_id, counters):
    """Runs one task for an entity, then starts the task its router chooses."""
    service, resource = TASKS[name]
    path_id = f"Shop.service.{service}"

    def publish(value):
        env.publish_event(entity.key, value, task=name, instance=task_id)

    if resource is not None:
        publish({"path_id": path_id, "status": "queued"})
    unit = shop[resource].request() if resource else contextlib.nullcontext()
    with unit as req:
        if req is not None:
            yield req
        publish({"path_id": path_id, "status": "started"})
        finished = payload(name, entity)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish(finished)

    entity.attributes.update(SETS.get(name, {}))
    upstream = Upstream(task_id, entity.key, entity.attributes, finished)
    following = ROUTES[name].choose(upstream, env)
    if following is not None:
        instance = next(counters[following])
        env.process(task(env, sampler, shop, following, entity, instance, counters))


def order_generator(env, sampler, shop, arrival, counters):
    """Starts an entity for every arrival, keyed orders-1, gifts-1 and so on."""
    gap = env.registry.get_config(f"Shop.arrival.{arrival}")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        entity = Entity(f"{arrival}-{number}", arrival, dict(ATTRIBUTES[arrival]))
        instance = next(counters["pick"])
        env.process(task(env, sampler, shop, "pick", entity, instance, counters))


def build(env):
    """Builds the pickers and packers."""
    return {
        "pickers": DynamicResource(env, "Shop", "pickers"),
        "packers": DynamicResource(env, "Shop", "packers"),
    }


def processes(env, sampler, shop):
    """The two arrivals, sharing one counter per task with the routing."""
    # One random sequence of its own for pack's routing, as the builder uses
    ROUTES["pack"].rng = sampler.generator("route.pack")
    counters = {name: itertools.count() for name in TASKS}
    return [
        order_generator(env, sampler, shop, "orders", counters),
        order_generator(env, sampler, shop, "gifts", counters),
    ]


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    sampler = Sampler(seed=42)
    shop = build(env)
    for process in processes(env, sampler, shop):
        env.process(process)
    try:
        env.run(until=120)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
