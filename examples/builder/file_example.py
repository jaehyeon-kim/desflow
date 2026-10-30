"""File steering, builder API, no containers.

The builder version of `yaml/file.yaml`. This example models a shop packing orders,
and orders arrive faster than one packer can pack them. `FileIngress` reads
../params.yaml, whose one line sets the packers' capacity. Open that file in an
editor while the run goes, change the 1 to 5 and save it. The packers' capacity and
queue_length, which the statistics publish every 2 seconds, show the effect within
about a second.

Needs no containers. The run goes in real time and ends after 60 seconds.
"""

import logging
from pathlib import Path

from desflow import ConsoleEgress, FileIngress, SimulationContext

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

PARAMS_FILE = Path(__file__).resolve().parent.parent / "params.yaml"

app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_egress(ConsoleEgress())
    # --8<-- [start:file-ingress]
    # Applies an entry of the file when it first appears or its value changes
    .add_ingress(FileIngress(PARAMS_FILE))
    # --8<-- [end:file-ingress]
    .add_resource("packers", current_cap=1, max_cap=5)
    .add_service("packing", dist="normal", mean=3.0, std=0.5)
    .add_arrival("orders", dist="exponential", rate=1.0)
    .with_stats("minimal", interval=2.0)
)


@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int, context):
    """Packs one order and returns the payload of its finished event."""
    return {"event_type": "order_packed", "shipping": "standard", "order_id": task_id}


@app.arrival_loop("orders")
def arrival_generator(context):
    """Spawns a pack_order task for every order the 'orders' distribution draws."""
    task_id = 0
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(task_id, context, entity=entity))
        task_id += 1


def run():
    """Runs 60 seconds in real time."""
    app.run(until=60)


if __name__ == "__main__":
    run()
