"""Write a Connector, low-level API: the shop with the CSV egress and the JSON Lines
ingress of write_a_connector_logic.py.

The low-level version of write_a_connector.py. Before the run, changes.jsonl holds one
change that sets the packers to 2. `low_level_form` runs the shop in the current
folder and returns the packers' capacity at the end and the rows of events.csv.
"""

import csv
import json
import logging
import sys
from pathlib import Path

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    SimParameter,
)

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


def low_level_form():
    """The same shop with the environment and the resource."""
    Path("changes.jsonl").write_text(json.dumps(CHANGE) + "\n")
    # --8<-- [start:low-level]
    env = DynamicRealtimeEnvironment(factor=0.05)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            service={"packing": DistributionConfig(dist="constant", value=1.5)},
            resources={"packers": CapacityConfig(current_cap=1, max_cap=3)},
        )
    )
    env.setup_ingress([JsonlIngress("changes.jsonl")])
    env.setup_egress([CsvEgress("events.csv", columns=["status", "event_type"])])
    packers = DynamicResource(env, "Shop", "packers")

    def order(env, number):
        key, path_id = f"orders-{number}", "Shop.service.packing"
        env.publish_event(
            key, {"path_id": path_id, "status": "queued"}, "pack_order", number - 1
        )
        with packers.request() as request:
            yield request
            env.publish_event(
                key, {"path_id": path_id, "status": "started"}, "pack_order", number - 1
            )
            yield env.timeout(1.5)
        env.publish_event(key, {"event_type": "order_packed"}, "pack_order", number - 1)

    def arrivals(env):
        number = 0
        while True:
            yield env.timeout(1.0)
            number += 1
            env.process(order(env, number))

    env.process(arrivals(env))
    try:
        env.run(until=20)
    finally:
        env.teardown()  # writes what is still buffered and stops the threads
    # --8<-- [end:low-level]
    return packers.capacity, _rows()


if __name__ == "__main__":
    capacity, rows = low_level_form()
    print(f"low_level_form  packers: {capacity}, rows in events.csv: {len(rows)}")
