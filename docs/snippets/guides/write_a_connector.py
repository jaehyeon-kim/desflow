"""Write a Connector: an egress that writes events to a CSV file and an ingress that
follows a JSON Lines file of changes.

An order arrives every second and each takes 1.5 seconds to pack, so one packer
falls behind. Before the run, changes.jsonl holds one change that sets the packers to
2. The ingress applies it at the start, and the events go to events.csv. Each
function runs the shop in the current folder, in YAML or in the builder, and
returns the packers' capacity at the end and the rows of events.csv.
write_a_connector_low_level.py is the low-level API.
"""

import csv
import json
import logging
import sys
from pathlib import Path

from desflow import SimulationContext

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))  # write_a_connector_logic.py is beside the blueprint
from write_a_connector_logic import CsvEgress, JsonlIngress  # noqa: E402

logging.basicConfig(
    level=logging.WARNING, format="%(levelname)s [%(asctime)s] %(message)s"
)
CHANGE = {"path": "Shop.resources.packers.current_cap", "value": 2}


def _rows():
    with open("events.csv", newline="") as file:
        return list(csv.DictReader(file))


def yaml_form():
    """The blueprint beside the classes."""
    Path("changes.jsonl").write_text(json.dumps(CHANGE) + "\n")
    app = SimulationContext.from_yaml(HERE / "write_a_connector.yaml")
    app.run()
    return app.get_resource("packers").capacity, _rows()


def builder_form():
    """The same shop with the builder."""
    Path("changes.jsonl").write_text(json.dumps(CHANGE) + "\n")
    # --8<-- [start:builder]
    app = (
        SimulationContext(sim_id="Shop", factor=0.05, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=3)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="constant", value=1.0)
        .add_ingress(JsonlIngress("changes.jsonl"))
        .add_egress(CsvEgress("events.csv", columns=["status", "event_type"]))
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_generator(context: SimulationContext):
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(context.next_instance("pack_order"), entity=order))

    app.run(until=20)
    # --8<-- [end:builder]
    return app.get_resource("packers").capacity, _rows()


if __name__ == "__main__":
    for form in (yaml_form, builder_form):
        capacity, rows = form()
        print(
            f"{form.__name__:15} packers: {capacity}, rows in events.csv: {len(rows)}"
        )
