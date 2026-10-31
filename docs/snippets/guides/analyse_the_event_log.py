"""Analyse the Event Log: a run written to Parquet, read back as an event log.

The shop runs for two minutes with one packer and writes its events to Parquet. The
script reads the files with pyarrow, turns each order's three events into the rows of
an event log (entity, event type, event, time, run), and computes each order's wait
and time in the shop and the run's throughput. It uses pyarrow only, no pandas.
python analyse_the_event_log.py writes the files to analyse_output/. With the
argument yaml, `run_yaml` runs analyse_the_event_log.yaml (beside the script) instead,
and analyse_the_event_log_low_level.py is the low-level API; each writes the same
events.
"""

import logging
import os
import shutil
import sys
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from pyarrow import Table

from desflow import ParquetStorageEgress, SimulationContext

logging.basicConfig(level=logging.WARNING)

RUN_SECONDS = 120


# --8<-- [start:run]
def shop(out: Path) -> SimulationContext:
    """One packer, orders about every 2 seconds, events written to Parquet in out."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=1)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.5)
        # One flat row per event, in files named events_<8 hex digits>.parquet
        .add_egress(ParquetStorageEgress(default_path=f"{out}/events.parquet"))
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int):
        # The finished event's value, so every event of a task has a status
        return {"status": "finished"}

    @app.arrival_loop("orders")
    def order_generator(context: SimulationContext):
        task_id = 0
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    return app


# --8<-- [end:run]


def run(out: Path) -> SimulationContext:
    """Runs the shop for two minutes into an empty folder and returns it."""
    shutil.rmtree(out, ignore_errors=True)  # a run adds files; start empty
    app = shop(out)
    app.run(until=RUN_SECONDS)
    return app


# --8<-- [start:run-yaml]
def run_yaml(
    out: Path, blueprint: str = "analyse_the_event_log.yaml"
) -> SimulationContext:
    """Runs the blueprint for two minutes into an empty folder and returns it."""
    shutil.rmtree(out, ignore_errors=True)  # a run adds files; start empty
    with patch.dict(os.environ, {"OUT": str(out)}):  # the folder the blueprint reads
        app = SimulationContext.from_yaml(blueprint)
    app.run()  # until 120, from the blueprint
    return app


# --8<-- [end:run-yaml]


# --8<-- [start:read]
def read_events(out: Path) -> Table:
    """Every file the sink wrote under out, as one table of events."""
    return ds.dataset(out, format="parquet").to_table()


# --8<-- [end:read]


# --8<-- [start:log]
# Each status of a task becomes these rows of the log, in this order
ROWS = {
    "queued": [("arrival_departure", "arrival"), ("queue", "waiting_for_packer")],
    "started": [("resource_use", "packing_start")],
    "finished": [("resource_use_end", "packing_end"), ("arrival_departure", "depart")],
}
ORDER = list(ROWS)  # queued, started, finished: the order of an order's events


def event_log(events: Table, run: int = 1) -> Table:
    """The event log: one row for each step an order took, sorted by time.

    The files are read in no fixed order, so the rows are sorted by time and, at the
    same time, by the order of the statuses.
    """
    packing = events.filter(pc.equal(events["task"], "pack_order")).to_pylist()
    packing.sort(key=lambda row: (row["sim_ts"], ORDER.index(row["status"])))
    rows = [
        {
            "entity_id": row["key"],
            "event_type": event_type,
            "event": event,
            "time": row["sim_ts"],
            "run": run,
        }
        for row in packing
        for event_type, event in ROWS[row["status"]]
    ]
    return Table.from_pylist(rows)


# --8<-- [end:log]


# --8<-- [start:waits]
def times(log: Table, event: str) -> dict:
    """The time of `event` for each order that reached it."""
    rows = log.filter(pc.equal(log["event"], event))
    return dict(zip(rows["entity_id"].to_pylist(), rows["time"].to_pylist()))


def waits_and_times(log: Table) -> dict:
    """Each order's wait, and its time in the shop if it left before the run ended."""
    arrived = times(log, "arrival")
    started = times(log, "packing_start")
    left = times(log, "depart")
    return {
        order: {
            "wait": round(started[order] - arrived[order], 3),
            "in_shop": round(left[order] - arrived[order], 3)
            if order in left
            else None,
        }
        for order in started
    }


def mean_wait(per_order: dict) -> float:
    """The mean wait of the orders that started packing, in seconds."""
    return pc.mean(pa.array([row["wait"] for row in per_order.values()])).as_py()


def throughput_per_minute(log: Table, seconds: float) -> float:
    """Orders that left the shop in each minute of the run."""
    return len(times(log, "depart")) / (seconds / 60)


# --8<-- [end:waits]


if __name__ == "__main__":
    folder = Path("analyse_output")
    run_yaml(folder) if sys.argv[1:] == ["yaml"] else run(folder)
    log = event_log(read_events(folder))
    per_order = waits_and_times(log)
    print(f"{log.num_rows} rows in the log, for {len(per_order)} orders")
    print(f"mean wait {mean_wait(per_order):.3f} s")
    print(f"throughput {throughput_per_minute(log, RUN_SECONDS):.1f} orders a minute")
