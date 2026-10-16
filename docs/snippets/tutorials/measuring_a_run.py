"""The Measuring a Run tutorial: the busy hour, with built-in statistics, builder.

Three packers, orders at 1.8 a second and the measured packing times. Every record,
the statistics included, is printed to the terminal, and the script prints the final
statistics again at the end.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

# The 40 packing times measured at the bench, in seconds
MEASURED = [
    1.5, 2.5, 2.4, 1.2, 1.3, 1.2, 1.8, 1.4, 2.0, 0.7,
    2.7, 1.4, 1.9, 1.4, 1.3, 1.8, 2.0, 1.4, 1.4, 1.9,
    1.1, 0.8, 1.7, 1.1, 0.7, 1.1, 1.2, 0.9, 0.8, 1.5,
    2.1, 1.3, 1.1, 1.7, 1.9, 1.3, 1.8, 2.2, 1.4, 1.1,
]  # fmt: skip

app = (
    SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    .add_resource("packers", current_cap=3, max_cap=10)
    .add_arrival("orders", dist="exponential", rate=1.8)
    .add_service("packing", dist="empirical", values=MEASURED)
    # --8<-- [start:stats]
    # Counts, time averages, and the mean and maximum wait and service, every 10 s
    .with_stats("typical")
    # --8<-- [end:stats]
    .add_egress(ConsoleEgress())
)


@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int):
    return {"event_type": "order_packed"}


@app.arrival_loop("orders")
def order_generator(context: SimulationContext):
    task_id = 0
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(task_id, entity=order))
        task_id += 1


# --8<-- [start:read]
if __name__ == "__main__":
    app.run(until="1 h")
    # The statistics are telemetry, so the console printed them; read them here too
    for name, value in app.env.stats.readings().items():
        print(f"{name} = {value}")
# --8<-- [end:read]
