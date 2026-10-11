"""The First Simulation tutorial: the factory, with the SimulationContext builder.

One lathe, parts arriving about every 2 seconds, and 1.5 seconds of machining per
part. Every record is printed to the terminal.
"""

import logging

from dynamic_des import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

# --8<-- [start:factory]
app = (
    # 1 simulated second = 1 real second
    SimulationContext(sim_id="Factory_A", factor=1.0)
    .add_resource("lathe", current_cap=1, max_cap=1)
    # With no std, a normal distribution returns its mean every time
    .add_service("machining", dist="normal", mean=1.5)
    # One part every 2 seconds on average
    .add_arrival("parts", dist="exponential", rate=0.5)
)
# --8<-- [end:factory]


# --8<-- [start:part]
@app.task(service_id="machining", resource_id="lathe")
def process_part(part_id: int):
    # Requests the lathe, waits the machining time, releases the lathe.
    # The returned dictionary is the finished event's value.
    return {"part_id": part_id}


@app.arrival_loop("parts")
def parts_generator(context: SimulationContext):
    part_id = 0
    while True:
        yield context.wait_for_arrival("parts")
        context.spawn(process_part(part_id))
        part_id += 1


# --8<-- [end:part]


# --8<-- [start:records]
app.add_egress(ConsoleEgress())
# --8<-- [end:records]

# --8<-- [start:run]
if __name__ == "__main__":
    app.run(until=10.0)
# --8<-- [end:run]
