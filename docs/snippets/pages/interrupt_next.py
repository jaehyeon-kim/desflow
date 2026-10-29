"""An order that loses its packer to a rush order is packed by hand instead.

The builder form of `on_interrupt_next`, and the same in the low-level API. Both run
the shop of Interruptions for 10 minutes and print how many orders were packed each
way.
"""

import itertools

import simpy

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
    SimulationContext,
)


def builder_shop() -> SimulationContext:
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource(
            "packers", current_cap=1, max_cap=1, queue="priority", preempt=True
        )
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.45, spawn="pack")
        .add_arrival("rush", dist="exponential", rate=0.15, spawn="rush_pack")
        .with_stats("typical")
    )

    # --8<-- [start:builder]
    @app.task(
        service_id="packing",
        resource_id="packers",
        priority=1,
        # An order that loses its packer to a rush order is packed by hand instead
        on_interrupt="abandon",
        on_interrupt_next="pack_by_hand",
    )
    def pack(task_id: int):
        return {"event_type": "packed"}

    # --8<-- [end:builder]

    @app.task(service_id="packing")
    def pack_by_hand(task_id: int):
        return {"event_type": "packed_by_hand"}

    @app.task(service_id="packing", resource_id="packers", priority=0)
    def rush_pack(task_id: int):
        return {"event_type": "rush_packed"}

    return app


PARAMS = SimParameter(
    sim_id="Shop",
    arrival={
        "orders": DistributionConfig(dist="exponential", rate=0.45),
        "rush": DistributionConfig(dist="exponential", rate=0.15),
    },
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
)


def pack_by_hand(env, sampler, counts):
    yield env.timeout(sampler.sample(env.registry.get_config("Shop.service.packing")))
    counts["by hand"] += 1


# --8<-- [start:low-level]
def pack(env, sampler, packers, counts):
    request = packers.request(priority=1)
    try:
        yield request
        yield env.timeout(
            sampler.sample(env.registry.get_config("Shop.service.packing"))
        )
        counts["packed"] += 1
    except simpy.Interrupt:
        # An order that loses its packer to a rush order is packed by hand instead
        env.process(pack_by_hand(env, sampler, counts))
    finally:
        packers.release(request)


# --8<-- [end:low-level]


def rush_pack(env, sampler, packers):
    with packers.request(priority=0) as request:
        yield request
        yield env.timeout(
            sampler.sample(env.registry.get_config("Shop.service.packing"))
        )


def arrivals(env, sampler, name, start):
    config = env.registry.get_config(f"Shop.arrival.{name}")
    for _ in itertools.count():
        yield env.timeout(sampler.sample(config))
        env.process(start())


def low_level_counts(until=600):
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    packers = DynamicResource(env, "Shop", "packers", queue="priority", preempt=True)
    sampler = Sampler(seed=42)
    counts = {"packed": 0, "by hand": 0}
    env.process(
        arrivals(env, sampler, "orders", lambda: pack(env, sampler, packers, counts))
    )
    env.process(
        arrivals(env, sampler, "rush", lambda: rush_pack(env, sampler, packers))
    )
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return counts


if __name__ == "__main__":
    app = builder_shop()
    app.run(until=600)
    r = app.env.stats.readings()
    print(
        f"builder: {r['Shop.stats.tasks.pack.finished']} packed, "
        f"{r['Shop.stats.tasks.pack_by_hand.finished']} by hand"
    )
    counts = low_level_counts()
    print(f"low-level: {counts['packed']} packed, {counts['by hand']} by hand")
    assert r["Shop.stats.tasks.pack_by_hand.finished"] > 0
    assert counts["by hand"] > 0
