"""The Streaming Out tutorial: the shop's records, to the terminal and to a file, builder.

One packer and 1.5 seconds of packing; orders arrive at 0.4 a second for an hour.
Every record is printed to the terminal, the finished orders are written to packed/,
and the script prints the files the sink wrote and one order's row:
python streaming_out.py 2>/dev/null shows only those lines.
"""

import json
import logging
import shutil
from pathlib import Path

from desflow import ConsoleEgress, JsonlStorageEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)


# --8<-- [start:packed]
def packed_only(record: dict) -> bool:
    """Keeps the finished orders: events whose value says order_packed."""
    return (
        record["stream_type"] == "event"
        and record["value"].get("event_type") == "order_packed"
    )
    # --8<-- [end:packed]


def shop(out: Path) -> SimulationContext:
    """The shop of Changing Demand, without the rush, writing under out."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=1)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.4)
        # The packers' and the packing task's readings every 10 minutes, as telemetry
        .with_stats("minimal", interval=600)
        # --8<-- [start:sinks]
        # Every event and every telemetry record
        .add_egress(ConsoleEgress())
        # The finished orders only, one flat row each
        .add_egress(
            JsonlStorageEgress(default_path=f"{out}/packed/orders.jsonl"),
            when=packed_only,
        )
        # --8<-- [end:sinks]
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

    return app


# --8<-- [start:read]
def read_files(folder: Path) -> dict:
    """The rows of each file the sink wrote to folder, by file name."""
    return {
        path.name: [json.loads(line) for line in path.read_text().splitlines()]
        for path in sorted(folder.glob("*.jsonl"))
    }
    # --8<-- [end:read]


def outcomes(out: Path) -> dict:
    """Runs the hour; returns the rows in each file and the row of orders-12."""
    shutil.rmtree(out / "packed", ignore_errors=True)  # a run adds files; start empty
    shop(out).run(until="1 h")
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
