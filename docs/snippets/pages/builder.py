"""The builder calls of the Builder API page, one section per group of methods.

The file is a complete simulation: tests/unit runs it unpaced for 30 simulation
seconds, so every call shown on the page is checked.
"""

from datetime import datetime, timedelta

from dynamic_des import ConsoleEgress, LocalIngress, SimulationContext

# --8<-- [start:clock]
app = SimulationContext(
    sim_id="Line_A",
    factor=0.0,  # 0.0 unpaced, 1.0 real time
    random_seed=42,  # the same draws on every run
    logical_start_time=datetime(2026, 10, 1, 9, 0),  # what simulation time 0 is
)
# --8<-- [end:clock]

# --8<-- [start:resources]
app.add_resource("lathe", current_cap=2, max_cap=5)  # 2 machines now, room for 5
app.add_container("coolant", current_cap=100.0, max_cap=200.0)  # litres in the tank
app.add_variable("conveyor_speed", 1.5)  # any other live value
# --8<-- [end:resources]

# --8<-- [start:distributions]
app.add_arrival("standard", dist="exponential", rate=0.5)  # a part every 2 s on average
app.add_service("milling", dist="truncated_normal", mean=3.0, std=1.0, low=0.5)
app.add_service("inspection", dist="constant", value=0.5)
# --8<-- [end:distributions]


# --8<-- [start:decorators]
@app.task(service_id="milling", resource_id="lathe")
def mill(part_id: int):
    # The finished event's value is exactly this dictionary
    return {"part_id": part_id, "status": "milled"}


@app.arrival_loop("standard")
def parts(context: SimulationContext):
    part_id = 0
    while True:
        yield context.wait_for_arrival("standard")
        context.spawn(mill(part_id))
        part_id += 1


@app.telemetry_loop(interval=5.0)
def report(context: SimulationContext):
    # Publishes Line_A.lathe.in_use every 5 simulation seconds
    context.publish("lathe.in_use", context.get_resource("lathe").in_use)


# --8<-- [end:decorators]


# --8<-- [start:process]
def speed_drift(context: SimulationContext, step: float):
    """Raises the conveyor speed a little every 10 simulation seconds."""
    path = "Line_A.variables.conveyor_speed"
    while True:
        yield context.env.timeout(10.0)
        speed = context.env.registry.get(path).value
        context.env.registry.update(path, speed + step)


app.add_process(speed_drift, step=0.1)
# --8<-- [end:process]

# --8<-- [start:connectors]
app.add_ingress(LocalIngress(schedule=[(5.0, "Line_A.resources.lathe.current_cap", 3)]))
app.add_egress(ConsoleEgress(), batch_size=100, flush_interval=1.0)
app.with_batching(batch_size=1_000, flush_interval=5.0)  # defaults for other sinks
# --8<-- [end:connectors]

# --8<-- [start:compile]
paths = app.compile_parameters()  # the SimParameter that run() registers
# --8<-- [end:compile]

if __name__ == "__main__":
    app.run(until=timedelta(seconds=30).total_seconds())
