"""The Builder forms of three statistics options of the Statistics page.

tests/docs/test_docs_results.py runs the three apps and checks their records.
"""

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
    SimulationContext,
)


def shop() -> SimulationContext:
    app = (
        SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
        .add_resource("packers", current_cap=2, max_cap=5)
        .add_service("packing", dist="gamma", shape=4, scale=0.75)
        .add_arrival("orders", dist="exponential", rate=0.6)
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def orders(context: SimulationContext):
        task_id = 0
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    return app


on_change = shop().add_egress(ConsoleEgress())
# --8<-- [start:on-change]
# The readings of level minimal, and one more set after each request and release
on_change.with_stats("minimal", on_change=True)
# --8<-- [end:on-change]

trace = shop()
# --8<-- [start:trace]
# Only a sink with when="trace" receives the trace records
trace.add_egress(ConsoleEgress(), when="trace")
trace.with_stats("none", trace=True)
# --8<-- [end:trace]

readings = shop().add_egress(ConsoleEgress())
# --8<-- [start:readings]
# Readings of your own: two expressions, evaluated every 30 seconds
readings.add_readings(
    30,
    {
        "waiting_per_packer": "queue.packers / max(registry['Shop.resources.packers.current_cap'], 1)",
        "arrival_rate": "registry['Shop.arrival.orders.rate']",
    },
)
# --8<-- [end:readings]


def orders_process(env, sampler, packers):
    gap = env.registry.get_config("Shop.arrival.orders")
    while True:
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order_process(env, sampler, packers))


def pack_order_process(env, sampler, packers):
    with packers.request() as request:
        yield request
        yield env.timeout(
            sampler.sample(env.registry.get_config("Shop.service.packing"))
        )


def low_level(until, sink, on_change=False, trace=False):
    """The same shop in the low-level API, with a reading at each change or the trace."""
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="exponential", rate=0.6)},
            service={"packing": DistributionConfig(dist="gamma", shape=4, scale=0.75)},
            resources={"packers": CapacityConfig(current_cap=2, max_cap=5)},
        )
    )
    env.setup_stats("Shop", "minimal" if on_change else "none")
    if on_change:
        # --8<-- [start:low-level-on-change]
        env.stats.on_change = True  # a reading after each request and release
        # --8<-- [end:low-level-on-change]
    if trace:
        # --8<-- [start:low-level-trace]
        env.setup_egress([sink], traces=[True])  # a trace sink gets only the trace
        env.trace_events()
        # --8<-- [end:low-level-trace]
    else:
        env.setup_egress([sink])
    packers = DynamicResource(env, "Shop", "packers")
    env.process(orders_process(env, Sampler(seed=42), packers))
    try:
        env.run(until=until)
    finally:
        env.teardown()


if __name__ == "__main__":
    for app in (on_change, trace, readings):
        app.run(until="2 min")
    low_level(120, ConsoleEgress(), on_change=True)
    low_level(10, ConsoleEgress(), trace=True)
