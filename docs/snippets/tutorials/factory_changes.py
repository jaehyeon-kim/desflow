"""The First Simulation tutorial, step 2, with the SimulationContext builder."""

import logging

from dynamic_des import ConsoleEgress, SimulationContext

logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s")

# --8<-- [start:random]
app = (
    SimulationContext(sim_id="Factory_A", factor=1.0, random_seed=42)  # same draws every run
    .add_resource("lathe", current_cap=1, max_cap=5)  # room for up to 5 lathes
    # Milling takes 3 seconds on average, give or take 0.5
    .add_service("milling", dist="normal", mean=3.0, std=0.5)
    .add_arrival("parts", dist="exponential", rate=0.5)
    .add_egress(ConsoleEgress())
)
# --8<-- [end:random]


@app.task(service_id="milling", resource_id="lathe")
def process_part(part_id: int):
    return {"part_id": part_id}


@app.arrival_loop("parts")
def parts_generator(context: SimulationContext):
    part_id = 0
    while True:
        yield context.wait_for_arrival("parts")
        context.spawn(process_part(part_id))
        part_id += 1


@app.telemetry_loop(interval=5.0)
def report(context: SimulationContext):
    context.publish("lathe_capacity", context.get_resource("lathe").capacity)


# --8<-- [start:changes]
def capacity_plan(context: SimulationContext):
    """Three lathes from t=10, two from t=20, on the simulation clock."""
    path = "Factory_A.resources.lathe.current_cap"
    yield context.env.timeout(10.0)
    context.env.registry.update(path, 3)
    yield context.env.timeout(10.0)
    context.env.registry.update(path, 2)


app.add_process(capacity_plan)
# --8<-- [end:changes]

if __name__ == "__main__":
    app.run(until=30.0)
