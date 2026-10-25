"""The Streaming Out tutorial: the shop's records, to the terminal and to a file, low-level.

One packer takes 1.5 seconds to pack an order, and an order arrives every 2.5 seconds
on average (rate 0.4), for an hour. Every record is printed to the terminal, and the
finished orders are written to packed/. The script then prints the files the sink
wrote and one order's row. To see only those lines, run:
python streaming_out_low_level.py 2>/dev/null
"""

import itertools
import json
import logging
import shutil
from pathlib import Path

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    JsonlStorageEgress,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.4)},
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
)


def packed_only(record: dict) -> bool:
    """Keeps the finished orders: events whose value says order_packed."""
    return (
        record["stream_type"] == "event"
        and record["value"].get("event_type") == "order_packed"
    )


def pack_order(env, sampler, packers, key, task_id):
    path_id = "Shop.service.packing"

    def publish(value):
        # The key names the order; task and instance name this run of the task
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack_order", task_id)
    with packers.request() as request:
        yield request
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})
        env.stats.task_finished("pack_order", task_id)


def order_generator(env, sampler, packers):
    arrival = env.registry.get_config("Shop.arrival.orders")
    for task_id in itertools.count():
        yield env.timeout(sampler.sample(arrival))
        key = f"orders-{task_id + 1}"
        env.process(pack_order(env, sampler, packers, key, task_id))


def run(out: Path):
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    # --8<-- [start:sinks]
    env.setup_egress(
        [
            # Every event and every telemetry record
            ConsoleEgress(),
            # The finished orders only, one flat row each
            JsonlStorageEgress(default_path=f"{out}/packed/orders.jsonl"),
        ],
        predicates=[None, packed_only],
    )
    # --8<-- [end:sinks]
    # The packers' and the packing task's readings every 10 minutes, as telemetry
    env.setup_stats("Shop", "minimal", interval=600)
    packers = DynamicResource(env, "Shop", "packers")
    env.process(order_generator(env, Sampler(seed=42), packers))
    # --8<-- [start:run]
    try:
        env.run(until=3600)  # the model stops at 1 hour
    finally:
        env.teardown()  # the sinks receive the rest and finish writing
    # --8<-- [end:run]
    return env


def read_files(folder: Path) -> dict:
    """The rows of each file the sink wrote to folder, by file name."""
    return {
        path.name: [json.loads(line) for line in path.read_text().splitlines()]
        for path in sorted(folder.glob("*.jsonl"))
    }


def outcomes(out: Path) -> dict:
    """Runs the hour; returns the rows in each file and the row of orders-12."""
    shutil.rmtree(out / "packed", ignore_errors=True)  # a run adds files; start empty
    run(out)
    files = read_files(out / "packed")
    rows = [row for file_rows in files.values() for row in file_rows]
    return {
        "file_rows": sorted(len(file_rows) for file_rows in files.values()),
        "row": next(row for row in rows if row["key"] == "orders-12"),
    }


if __name__ == "__main__":
    r = outcomes(Path("streaming_out_output"))
    print(f"{len(r['file_rows'])} files, with {r['file_rows']} rows")
    row = {k: v for k, v in r["row"].items() if k != "timestamp"}
    print(f"orders-12: {row}")
