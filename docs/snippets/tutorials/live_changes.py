"""The Live Changes tutorial: a label printer jam, written as a scenario, builder.

One packer takes 1.5 seconds to pack an order, and an order arrives every 2.5 seconds
on average (rate 0.4). From 20 to 30 minutes a jammed label printer slows packing to
3 seconds. The events are written to events/ and the ground-truth records of the jam
to labels/, and the script prints the mean waits with and without the jam.
Run it with: python live_changes.py
With the argument live, it sends the jam live: python live_changes.py live
With compare and two folders, it reads two runs that are already written, such as the
runs of live_changes.yaml with and without its scenario block:
python live_changes.py compare jam baseline
"""

import json
import logging
import shutil
import sys
from pathlib import Path

from desflow import JsonlStorageEgress, LocalIngress, SimulationContext

logging.basicConfig(level=logging.WARNING)


def shop(out: Path, jam: bool = True) -> SimulationContext:
    """The shop of Changing Demand, writing its events and labels under out."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=1)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.4)
        # --8<-- [start:sinks]
        # The events, one flat row per event
        .add_egress(JsonlStorageEgress(default_path=f"{out}/events/orders.jsonl"))
        # The ground-truth records only
        .add_egress(
            JsonlStorageEgress(default_path=f"{out}/labels/labels.jsonl"),
            when="labels",
        )
        # --8<-- [end:sinks]
    )
    if jam:
        # --8<-- [start:jam]
        # The jam: 3 seconds of packing from 20 to 30 minutes, each change recorded
        app.add_scenario(
            [
                {
                    "at": "20 min",
                    "path": "Shop.service.packing.value",
                    "value": 3.0,
                    "record": {"intervention": "printer_jam"},
                },
                {
                    "at": "30 min",
                    "path": "Shop.service.packing.value",
                    "value": 1.5,
                    "record": {"intervention": "printer_fixed"},
                },
            ]
        )
        # --8<-- [end:jam]

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
def read_rows(folder: Path) -> list:
    """Every row of every JSONL file the sink wrote to folder."""
    return [
        json.loads(line)
        for path in sorted(folder.glob("*.jsonl"))
        for line in path.read_text().splitlines()
    ]


def waits(out: Path) -> dict:
    """The wait of each order, by the time it was queued, in seconds."""
    queued, started = {}, {}
    for row in read_rows(out / "events"):
        if row.get("status") == "queued":
            queued[row["instance"]] = row["sim_ts"]
        elif row.get("status") == "started":
            started[row["instance"]] = row["sim_ts"]
    return {queued[i]: started[i] - queued[i] for i in started}


def mean_wait(by_time: dict, start: float, end: float) -> float:
    """The mean wait of the orders queued from start to end, in seconds."""
    window = [wait for at, wait in by_time.items() if start <= at < end]
    return sum(window) / len(window)
    # --8<-- [end:read]


def outcomes(out: Path) -> dict:
    """Runs the hour with and without the jam; returns the labels and mean waits."""
    for jam in (True, False):
        folder = out / ("jam" if jam else "baseline")
        shutil.rmtree(folder, ignore_errors=True)  # a run adds files; start empty
        shop(folder, jam).run(until="1 h")
    return compare(out / "jam", out / "baseline")


def compare(jam_folder: Path, baseline_folder: Path) -> dict:
    """Reads the two runs' files; returns the labels and mean waits."""
    result: dict = {}
    by_run = {True: waits(jam_folder), False: waits(baseline_folder)}
    for jam, by_time in by_run.items():
        result[jam] = {
            "jam": round(mean_wait(by_time, 1200, 1800), 2),
            "after": round(mean_wait(by_time, 1800, 2400), 2),
        }
    # Both runs meet the same orders, so each order's two waits can be compared
    result["last_longer"] = max(
        at for at, wait in by_run[True].items() if wait > by_run[False][at] + 0.001
    )
    labels = read_rows(jam_folder / "labels")
    result["labels"] = [
        (row["sim_ts"], row["label"]["intervention"], row["old"], row["new"])
        for row in sorted(labels, key=lambda row: row["sim_ts"])
    ]
    return result


def report(result: dict) -> None:
    """Prints the labels and the mean waits."""
    for at, name, old, new in result["labels"]:
        print(f"label at {at:6.1f} s: {name}, packing {old} s to {new} s")
    for jam, label in ((True, "with the jam"), (False, "without it")):
        print(
            f"{label:>12}: mean wait {result[jam]['jam']:6.2f} s from 20 to 30 min, "
            f"{result[jam]['after']:6.2f} s from 30 to 40 min"
        )
    print(
        f"the last order that waits longer is queued at {result['last_longer']:.1f} s"
    )


def live(out: Path) -> SimulationContext:
    """The same jam sent live, half a second of wall-clock time after the start."""
    app = shop(out, jam=False)
    app.factor = 0.005
    # --8<-- [start:live]
    # Half a wall-clock second after the start, set packing to 3 seconds
    app.add_ingress(LocalIngress(schedule=[(0.5, "Shop.service.packing.value", 3.0)]))
    # --8<-- [end:live]
    # Flush every 60 simulated seconds: at this pace the default of 1 s flushes on a
    # timer over and over, and desflow warns about it
    app.with_batching(batch_size=500, flush_interval=60)
    app.run(until="5 min")
    return app


if __name__ == "__main__":
    here = Path("live_changes_output")
    if sys.argv[1:] == ["live"]:
        # python live_changes.py live: the jam sent live instead
        # A run adds files; start empty
        shutil.rmtree(here / "live", ignore_errors=True)
        env = live(here / "live").env
        packing = env.registry.get_config("Shop.service.packing").value
        print(f"packing after the live change: {packing} s")
    elif sys.argv[1:2] == ["compare"]:
        # python live_changes.py compare jam baseline: two runs that are already written
        report(compare(Path(sys.argv[2]), Path(sys.argv[3])))
    else:
        report(outcomes(here))
