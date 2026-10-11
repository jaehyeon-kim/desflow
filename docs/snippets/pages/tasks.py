"""The tasks and processes of the Tasks and Processes page: builder and low-level.

tests/unit runs this file: both versions run unpaced for 3 simulation minutes.
"""

from dynamic_des import (
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
    SimulationContext(sim_id="Workshop", factor=0.0, random_seed=1)
    .add_resource("drill", current_cap=1, max_cap=3)
    .add_service("drilling", dist="normal", mean=4.0, std=1.0)
    .add_arrival("parts", dist="exponential", rate=0.2)
    .add_arrival("orders", dist="exponential", rate=0.1)
    .add_egress(ConsoleEgress())
)


# --8<-- [start:builder-task]
@app.task(service_id="drilling", resource_id="drill")
def drill_part(part_id: int):
    # Runs when the part gets the drill, before the drilling time.
    # The returned dict is the finished event's value.
    return {"status": "finished", "part_id": part_id}


@app.task()  # no service and no resource: publishes at once
def order_placed(order_id: int):
    return {"status": "placed", "order_id": order_id}


# --8<-- [end:builder-task]


# --8<-- [start:builder-arrival]
@app.arrival_loop("parts")
def parts(context: SimulationContext):
    part_id = 0
    while True:
        yield context.wait_for_arrival("parts")  # draws the next gap, then waits
        context.spawn(drill_part(part_id))  # starts the task; the loop goes on
        part_id += 1


# --8<-- [end:builder-arrival]


@app.arrival_loop("orders")
def orders(context: SimulationContext):
    order_id = 0
    while True:
        yield context.wait_for_arrival("orders")
        context.spawn(order_placed(order_id))
        order_id += 1


# --8<-- [start:builder-process]
def maintenance(
    context: SimulationContext, resource: str, every: float, duration: float
):
    """Takes the machine out of service for `duration` seconds every `every` seconds."""
    path = f"{context.sim_id}.resources.{resource}.current_cap"
    registry = context.env.registry
    while True:
        yield context.env.timeout(every)
        normal = registry.get(path).value
        registry.update(path, 0)
        yield context.env.timeout(duration)
        registry.update(path, normal)


app.add_process(maintenance, resource="drill", every=50, duration=5)
# --8<-- [end:builder-process]


# --8<-- [start:builder-telemetry]
@app.telemetry_loop(interval=10.0)
def drill_stats(context: SimulationContext):
    drill = context.get_resource("drill")
    context.publish("drill_busy", drill.in_use)  # as Workshop.drill_busy
    context.publish("drill_waiting", len(drill.queue.items))  # requests waiting


# --8<-- [end:builder-telemetry]


# --8<-- [start:low-level-task]
def drill_part_process(env, sampler, drill, part_id):
    """What @app.task does, by hand."""
    key, path_id = f"task-{part_id}", "Workshop.service.drilling"
    env.publish_event(key, {"path_id": path_id, "status": "queued"})
    with drill.request() as request:
        yield request
        env.publish_event(key, {"path_id": path_id, "status": "started"})
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        env.publish_event(key, {"status": "finished", "part_id": part_id})


# --8<-- [end:low-level-task]


# --8<-- [start:low-level-process]
def maintenance_process(env, resource: str, every: float, duration: float):
    """What add_process starts, by hand: a plain SimPy generator."""
    path = f"Workshop.resources.{resource}.current_cap"
    while True:
        yield env.timeout(every)
        normal = env.registry.get(path).value
        env.registry.update(path, 0)
        yield env.timeout(duration)
        env.registry.update(path, normal)


# --8<-- [end:low-level-process]


# --8<-- [start:low-level-arrival]
def parts_process(env, sampler, drill):
    """What @app.arrival_loop does, by hand."""
    gap = env.registry.get_config("Workshop.arrival.parts")
    part_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        env.process(drill_part_process(env, sampler, drill, part_id))
        part_id += 1


# --8<-- [end:low-level-arrival]


# --8<-- [start:low-level-telemetry]
def drill_stats_process(env, drill):
    """What @app.telemetry_loop does, by hand."""
    while True:
        env.publish_telemetry("Workshop.drill_busy", drill.in_use)
        env.publish_telemetry("Workshop.drill_waiting", len(drill.queue.items))
        yield env.timeout(10.0)


# --8<-- [end:low-level-telemetry]


def run_low_level(until):
    import numpy as np

    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Workshop",
            arrival={"parts": DistributionConfig(dist="exponential", rate=0.2)},
            service={"drilling": DistributionConfig(dist="normal", mean=4.0, std=1.0)},
            resources={"drill": CapacityConfig(current_cap=1, max_cap=3)},
        )
    )
    env.setup_egress([ConsoleEgress()])
    drill = DynamicResource(env, "Workshop", "drill")
    sampler = Sampler(rng=np.random.default_rng(1))
    # --8<-- [start:low-level-start]
    env.process(parts_process(env, sampler, drill))  # start each loop once
    env.process(drill_stats_process(env, drill))
    env.process(maintenance_process(env, "drill", every=50, duration=5))
    # --8<-- [end:low-level-start]
    try:
        env.run(until=until)
    finally:
        env.teardown()


if __name__ == "__main__":
    app.run(until="3 min")
    run_low_level(until=180)
