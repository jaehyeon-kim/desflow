"""The Live Changes tutorial: a label printer jam, written as a scenario, low-level.

One packer and 1.5 seconds of packing; orders arrive at 0.4 a second. From 20 to 30
minutes a jammed label printer slows packing to 3 seconds. The events are written to
events/ and the ground-truth records of the jam to labels/, and the script prints
the mean waits with and without the jam: python live_changes_low_level.py
With the argument live, it sends the jam live: python live_changes_low_level.py live
"""

import copy
import itertools
import json
import logging
import shutil
import sys
from pathlib import Path

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    JsonlStorageEgress,
    LocalIngress,
    Sampler,
    SimParameter,
    run_scenario,
)

logging.basicConfig(level=logging.WARNING)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.4)},
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
)

# --8<-- [start:jam]
# The jam: 3 seconds of packing from 20 to 30 minutes, each change recorded
JAM = [
    {
        "at": 1200,
        "path": "Shop.service.packing.value",
        "value": 3.0,
        "record": {"intervention": "printer_jam"},
    },
    {
        "at": 1800,
        "path": "Shop.service.packing.value",
        "value": 1.5,
        "record": {"intervention": "printer_fixed"},
    },
]
# --8<-- [end:jam]


def pack_order(env, sampler, packers, key, task_id):
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    with packers.request() as request:
        yield request
        publish({"path_id": path_id, "status": "started"})
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})


def order_generator(env, sampler, packers):
    arrival = env.registry.get_config("Shop.arrival.orders")
    for task_id in itertools.count():
        yield env.timeout(sampler.sample(arrival))
        key = f"orders-{task_id + 1}"
        env.process(pack_order(env, sampler, packers, key, task_id))


def run(
    out: Path, jam: bool = True, factor: float = 0.0, until: float = 3600, ingress=None
):
    # strict=False lets a paced run fall behind the wall clock without stopping
    env = DynamicRealtimeEnvironment(factor=factor, strict=False)
    # A copy, because a change to the registry changes the parameters it holds
    env.registry.register_sim_parameter(copy.deepcopy(PARAMS))
    # --8<-- [start:sinks]
    env.setup_egress(
        [
            # The events, one flat row per event
            JsonlStorageEgress(default_path=f"{out}/events/orders.jsonl"),
            # The ground-truth records only
            JsonlStorageEgress(default_path=f"{out}/labels/labels.jsonl"),
        ],
        labels=[False, True],
    )
    # --8<-- [end:sinks]
    if ingress:
        env.setup_ingress(ingress)
    packers = DynamicResource(env, "Shop", "packers")
    if jam:
        # --8<-- [start:run]
        # Each step applies before every other event at its time
        env.process(run_scenario(env, JAM, until=until))
        # --8<-- [end:run]
    env.process(order_generator(env, Sampler(seed=42), packers))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


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


def outcomes(out: Path) -> dict:
    """Runs the hour with and without the jam; returns the labels and mean waits."""
    result: dict = {}
    by_run: dict = {}
    for jam in (True, False):
        folder = out / ("jam" if jam else "baseline")
        shutil.rmtree(folder, ignore_errors=True)  # a run adds files; start empty
        run(folder, jam)
        by_time = by_run[jam] = waits(folder)
        result[jam] = {
            "jam": round(mean_wait(by_time, 1200, 1800), 2),
            "after": round(mean_wait(by_time, 1800, 2400), 2),
        }
    # Both runs meet the same orders, so each order's two waits can be compared
    result["last_longer"] = max(
        at for at, wait in by_run[True].items() if wait > by_run[False][at] + 0.001
    )
    labels = read_rows(out / "jam" / "labels")
    result["labels"] = [
        (row["sim_ts"], row["label"]["intervention"], row["old"], row["new"])
        for row in sorted(labels, key=lambda row: row["sim_ts"])
    ]
    return result


def live(out: Path):
    """The same jam sent live, half a second of wall-clock time after the start."""
    # --8<-- [start:live]
    # Half a wall-clock second after the start, set packing to 3 seconds
    ingress = [LocalIngress(schedule=[(0.5, "Shop.service.packing.value", 3.0)])]
    # --8<-- [end:live]
    return run(out, jam=False, factor=0.005, until=300, ingress=ingress)


if __name__ == "__main__":
    here = Path("live_changes_output")
    if sys.argv[1:] == ["live"]:
        # python live_changes_low_level.py live: the jam sent live instead
        # A run adds files; start empty
        shutil.rmtree(here / "live", ignore_errors=True)
        env = live(here / "live")
        packing = env.registry.get_config("Shop.service.packing").value
        print(f"packing after the live change: {packing} s")
    else:
        r = outcomes(here)
        for at, name, old, new in r["labels"]:
            print(f"label at {at:6.1f} s: {name}, packing {old} s to {new} s")
        for jam, label in ((True, "with the jam"), (False, "without it")):
            print(
                f"{label:>12}: mean wait {r[jam]['jam']:6.2f} s from 20 to 30 min, "
                f"{r[jam]['after']:6.2f} s from 30 to 40 min"
            )
        print(f"the last order that waits longer is queued at {r['last_longer']:.1f} s")
