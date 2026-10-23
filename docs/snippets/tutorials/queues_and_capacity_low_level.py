"""The Queues and Capacity tutorial: the shop's busy hour, on the low-level API.

Orders arrive at 1.8 a second and each takes 1.5 seconds to pack. The script runs the
hour once for each number of packers, unpaced, and prints the mean wait of each run.
`run_network` is the shop with a checker and rework, and `run_rush` the half hour
that a rush overloads.
"""

import itertools

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Router,
    Sampler,
    SimParameter,
    Upstream,
    run_scenario,
)

UNTIL = 3600  # the busy hour, in seconds


def params(packers: int) -> SimParameter:
    """The shop in its busy hour, with `packers` packers."""
    return SimParameter(
        sim_id="Shop",
        # --8<-- [start:load]
        service={"packing": DistributionConfig(dist="constant", value=1.5)},
        # The busy hour: 1.8 orders a second, more than one packer can pack
        arrival={"orders": DistributionConfig(dist="exponential", rate=1.8)},
        # --8<-- [end:load]
        # --8<-- [start:packers]
        resources={"packers": CapacityConfig(current_cap=packers, max_cap=10)},
        # --8<-- [end:packers]
    )


def pack_order(env, sampler, packers, task_id):
    """One order, which reports each step to the statistics."""
    path_id = "Shop.service.packing"
    env.stats.task_queued("pack_order", task_id)
    with packers.request() as request:
        yield request
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
    env.stats.task_finished("pack_order", task_id)


def order_generator(env, sampler, packers):
    arrival = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(pack_order(env, sampler, packers, task_id))
        task_id += 1


def run(packers: int, until=UNTIL, factor=0.0):
    """Runs the busy hour with `packers` packers, unpaced, and returns the environment."""
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(params(packers))
    # --8<-- [start:stats]
    # The mean wait of pack_order, kept by the environment and read at the end
    env.setup_stats("Shop", "typical", interval=3600)
    # --8<-- [end:stats]
    packers_resource = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(seed=42)
    env.process(order_generator(env, sampler, packers_resource))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


# --8<-- [start:network]
NETWORK = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=1.6)},
    service={
        "packing": DistributionConfig(dist="constant", value=1.5),
        # 0.4 s on average
        "checking": DistributionConfig(dist="exponential", rate=2.5),
    },
    resources={
        "packers": CapacityConfig(current_cap=4, max_cap=10),
        "checkers": CapacityConfig(current_cap=1, max_cap=4),
    },
)
# Each task's service and resource, and the task after it
STEPS = {
    "pack_order": ("packing", "packers", Router("check_order")),
    # Nine in ten pass; the tenth is packed again
    "check_order": (
        "checking",
        "checkers",
        Router({"weights": {"ship_order": 9, "pack_order": 1}}),
    ),
    "ship_order": (None, None, Router(None)),
}


def step(env, sampler, shop, name, entity, counters):
    """One task of an order, then the task its router chooses."""
    service, resource, router = STEPS[name]
    task_id = next(counters[name])
    if resource is None:
        env.stats.task_finished(name, task_id)
    else:
        env.stats.task_queued(name, task_id)
        with shop[resource].request() as request:
            yield request
            env.stats.task_started(name, task_id)
            config = env.registry.get_config(f"Shop.service.{service}")
            yield env.timeout(sampler.sample(config))
        env.stats.task_finished(name, task_id)
    following = router.choose(Upstream(task_id, entity, {}, {}), env)
    if following is None:
        env.stats.flow_finished("orders")
    else:
        env.process(step(env, sampler, shop, following, entity, counters))


# --8<-- [end:network]


def network_orders(env, sampler, shop, counters):
    arrival = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(arrival))
        env.stats.flow_started("orders")
        env.process(
            step(env, sampler, shop, "pack_order", f"orders-{number}", counters)
        )


def run_network(until=UNTIL):
    """Runs the network for the hour, unpaced, and returns the environment."""
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(NETWORK)
    env.setup_stats("Shop", "typical", interval=3600)
    shop = {
        name: DynamicResource(env, "Shop", name) for name in ("packers", "checkers")
    }
    sampler = Sampler(seed=42)
    # Weights draw from the routing task's own sequence, as the builder's do
    STEPS["check_order"][2].rng = sampler.generator("route.check_order")
    counters = {name: itertools.count() for name in STEPS}
    env.process(network_orders(env, sampler, shop, counters))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


def run_rush(until=1800):
    """Three packers until a rush at 10 minutes; returns the environment."""
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(params(3))
    # --8<-- [start:rush]
    # The rush: 2.4 orders a second from 10 minutes, utilisation 2.4 x 1.5 / 3 = 1.2
    rush = [{"at": 600, "path": "Shop.arrival.orders.rate", "value": 2.4}]
    env.process(run_scenario(env, rush, until=until))
    # The queue length, published every 5 minutes
    env.setup_stats("Shop", "minimal", interval=300)
    # --8<-- [end:rush]
    packers = DynamicResource(env, "Shop", "packers")
    env.process(order_generator(env, Sampler(seed=42), packers))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


# --8<-- [start:table]
def mean_wait(packers: int) -> float:
    """Runs the busy hour with `packers` packers and returns the mean wait."""
    env = run(packers)
    return env.stats.readings()["Shop.stats.tasks.pack_order.wait_mean"]


if __name__ == "__main__":
    for packers in range(1, 5):
        print(f"packers {packers}: mean wait {mean_wait(packers):8.2f} s")
# --8<-- [end:table]
