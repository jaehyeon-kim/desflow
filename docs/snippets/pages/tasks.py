"""The tasks and processes of the Tasks and Processes page: builder and low-level.

tests/unit runs this file: both versions run unpaced for 3 simulation minutes.
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
    SimulationContext(sim_id="Shop", factor=0.0, random_seed=1)
    .add_resource("packers", current_cap=1, max_cap=3)
    .add_service("packing", dist="normal", mean=4.0, std=1.0)
    .add_arrival("orders", dist="exponential", rate=0.2)
    .add_arrival("returns", dist="exponential", rate=0.1)
    .add_egress(ConsoleEgress())
)


# --8<-- [start:builder-task]
@app.task(service_id="packing", resource_id="packers")
def pack_order(order_id: int):
    # Runs when the order gets a packer, before the packing time.
    # The returned dict is the finished event's value.
    return {"status": "finished", "order_id": order_id}


@app.task()  # no service and no resource: publishes at once
def return_logged(return_id: int):
    return {"status": "logged", "return_id": return_id}


# --8<-- [end:builder-task]


# --8<-- [start:builder-arrival]
@app.arrival_loop("orders")
def orders(context: SimulationContext):
    order_id = 0
    while True:
        yield context.wait_for_arrival("orders")  # draws the next gap, then waits
        context.spawn(pack_order(order_id))  # starts the task; the loop goes on
        order_id += 1


# --8<-- [end:builder-arrival]


@app.arrival_loop("returns")
def returns(context: SimulationContext):
    return_id = 0
    while True:
        yield context.wait_for_arrival("returns")
        context.spawn(return_logged(return_id))
        return_id += 1


# --8<-- [start:builder-process]
def breaks(context: SimulationContext, resource: str, every: float, duration: float):
    """Takes every packer off for a break of `duration` seconds every `every` seconds."""
    path = f"{context.sim_id}.resources.{resource}.current_cap"
    registry = context.env.registry
    while True:
        yield context.env.timeout(every)
        normal = registry.get(path).value
        registry.update(path, 0)
        yield context.env.timeout(duration)
        registry.update(path, normal)


app.add_process(breaks, resource="packers", every=50, duration=5)
# --8<-- [end:builder-process]


# --8<-- [start:builder-telemetry]
@app.telemetry_loop(interval=10.0)
def packer_stats(context: SimulationContext):
    packers = context.get_resource("packers")
    context.publish("packers_busy", packers.in_use)  # as Shop.packers_busy
    context.publish("packers_waiting", len(packers.queue.items))  # requests waiting


# --8<-- [end:builder-telemetry]


# --8<-- [start:low-level-task]
def pack_order_process(env, sampler, packers, order_id):
    """What @app.task does, by hand."""
    key, path_id = f"task-{order_id}", "Shop.service.packing"
    env.publish_event(key, {"path_id": path_id, "status": "queued"})
    with packers.request() as request:
        yield request
        env.publish_event(key, {"path_id": path_id, "status": "started"})
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        env.publish_event(key, {"status": "finished", "order_id": order_id})


# --8<-- [end:low-level-task]


# --8<-- [start:low-level-process]
def breaks_process(env, resource: str, every: float, duration: float):
    """What add_process starts, by hand: a plain SimPy generator."""
    path = f"Shop.resources.{resource}.current_cap"
    while True:
        yield env.timeout(every)
        normal = env.registry.get(path).value
        env.registry.update(path, 0)
        yield env.timeout(duration)
        env.registry.update(path, normal)


# --8<-- [end:low-level-process]


# --8<-- [start:low-level-arrival]
def orders_process(env, sampler, packers):
    """What @app.arrival_loop does, by hand."""
    gap = env.registry.get_config("Shop.arrival.orders")
    order_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order_process(env, sampler, packers, order_id))
        order_id += 1


# --8<-- [end:low-level-arrival]


# --8<-- [start:low-level-telemetry]
def packer_stats_process(env, packers):
    """What @app.telemetry_loop does, by hand."""
    while True:
        env.publish_telemetry("Shop.packers_busy", packers.in_use)
        env.publish_telemetry("Shop.packers_waiting", len(packers.queue.items))
        yield env.timeout(10.0)


# --8<-- [end:low-level-telemetry]


def run_low_level(until):
    import numpy as np

    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="exponential", rate=0.2)},
            service={"packing": DistributionConfig(dist="normal", mean=4.0, std=1.0)},
            resources={"packers": CapacityConfig(current_cap=1, max_cap=3)},
        )
    )
    env.setup_egress([ConsoleEgress()])
    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(rng=np.random.default_rng(1))
    # --8<-- [start:low-level-start]
    env.process(orders_process(env, sampler, packers))  # start each loop once
    env.process(packer_stats_process(env, packers))
    env.process(breaks_process(env, "packers", every=50, duration=5))
    # --8<-- [end:low-level-start]
    try:
        env.run(until=until)
    finally:
        env.teardown()


if __name__ == "__main__":
    app.run(until="3 min")
    run_low_level(until=180)
