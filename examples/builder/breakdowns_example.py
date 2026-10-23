"""Breakdowns: a packing station that fails and is repaired, builder API.

The packing station breaks down about once a minute and takes 5 to 15 seconds to
repair. A breakdown stops the orders being packed: each gives the station back,
queues again, and packs what it had left once the station is repaired.

Needs no containers. Runs unpaced and ends after 300 simulation seconds.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

app = (
    SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    .add_egress(ConsoleEgress())
    # --8<-- [start:station]
    .add_resource("station", current_cap=2, max_cap=3)
    .add_breakdown(
        "station",
        # Calendar time from a repair, or the start, to the next failure
        # A failure every 60 s on average (rate 0.0167)
        time_to_failure={"dist": "exponential", "rate": 0.0167},
        repair={"dist": "uniform", "low": 5, "high": 15},
        # The orders being packed are interrupted, and follow their on_interrupt
        work="interrupt",
    )
    # --8<-- [end:station]
    .add_service("packing", dist="triangular", low=1, mode=2, high=4)
    # An order every 1.67 s on average (rate 0.6)
    .add_arrival("orders", dist="exponential", rate=0.6)
    # Every 60 seconds: the station's down state, downtime and failures
    .with_stats("basic", interval=60.0)
)


# Packs what it had left once the station is back
@app.task(service_id="packing", resource_id="station", on_interrupt="resume")
def pack(task_id, context):
    return {"event_type": "order_packed"}


@app.arrival_loop("orders")
def order_loop(context):
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack(context.next_instance("pack"), context, entity=entity))


def run():
    """Runs 300 simulation seconds."""
    app.run(until=300)


if __name__ == "__main__":
    run()
