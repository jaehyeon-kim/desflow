"""The statistics of the Statistics page: builder and low-level.

tests/docs runs this file: both versions run unpaced for 2 simulation minutes, and the
low-level one checks the readings its process reports.
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

app = (
    SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
    .add_resource("packers", current_cap=2, max_cap=5)
    .add_service("packing", dist="gamma", shape=4, scale=0.75)
    # An order every 1.67 s on average (rate 0.6)
    .add_arrival("orders", dist="exponential", rate=0.6)
    .add_egress(ConsoleEgress())
)
# --8<-- [start:builder-levels]
# none (the default), minimal, basic, typical or all, each adding to the one before;
# published every interval of simulation seconds and once at the end
app.with_stats("typical", interval=60)
# --8<-- [end:builder-levels]


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


# --8<-- [start:builder-custom]
@app.telemetry_loop(interval=30.0)
def packers_report(context: SimulationContext):
    """Readings of your own: orders waiting per packer, and the live arrival rate."""
    packers = context.get_resource("packers")
    waiting = len(packers.queue)
    context.publish("waiting_per_packer", waiting / max(packers.capacity, 1))
    rate = context.env.registry.get("Shop.arrival.orders.rate").value
    context.publish("arrival_rate", rate)  # as Shop.arrival_rate


# --8<-- [end:builder-custom]


def pack_order_process(env, sampler, packers, key, task_id):
    path_id = "Shop.service.packing"
    # --8<-- [start:low-level-report]
    # A low-level task reports its own steps to the statistics
    env.stats.task_queued("pack_order", task_id)
    with packers.request() as request:
        yield request
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
    env.stats.task_finished("pack_order", task_id)
    env.stats.flow_finished("orders")  # the order leaves the shop
    # --8<-- [end:low-level-report]


def orders_process(env, sampler, packers):
    gap = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        env.process(
            pack_order_process(env, sampler, packers, f"orders-{task_id + 1}", task_id)
        )
        task_id += 1


def run_low_level(until=120):
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            # An order every 1.67 s on average (rate 0.6)
            arrival={"orders": DistributionConfig(dist="exponential", rate=0.6)},
            service={"packing": DistributionConfig(dist="gamma", shape=4, scale=0.75)},
            resources={"packers": CapacityConfig(current_cap=2, max_cap=5)},
        )
    )
    # --8<-- [start:low-level-levels]
    env.setup_stats("Shop", "typical", interval=60)  # before or after the resources
    # --8<-- [end:low-level-levels]
    packers = DynamicResource(env, "Shop", "packers")  # adds itself to env.stats
    env.process(orders_process(env, Sampler(seed=42), packers))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    # --8<-- [start:low-level-read]
    # Every statistic the level publishes, by its published name
    readings = env.stats.readings()
    print(f"mean wait {readings['Shop.stats.tasks.pack_order.wait_mean']:.2f} s")
    # --8<-- [end:low-level-read]
    return readings


if __name__ == "__main__":
    app.run(until="2 min")
    readings = run_low_level()
    builder = app.env.stats.readings()
    assert readings["Shop.stats.tasks.pack_order.finished"] > 0
    assert (
        readings["Shop.stats.tasks.pack_order.wait_mean"]
        == builder["Shop.stats.tasks.pack_order.wait_mean"]
    )
