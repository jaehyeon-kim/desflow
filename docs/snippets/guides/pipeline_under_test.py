"""The pipeline under test and its scorer, for the Test a Pipeline page.

The pipeline reads the events of a run and flags the windows with unusually many orders.
The scorer compares what it flagged with the labels of the faults the run planted.
"""

import glob
import json
import statistics
from collections import Counter


def read_lines(folder: str) -> list[dict]:
    """Every JSON line of every file under `folder`, in time order."""
    lines = []
    for path in glob.glob(f"{folder}/*.jsonl"):
        with open(path) as file:
            lines += [json.loads(line) for line in file]
    return sorted(lines, key=lambda line: line["sim_ts"])


# --8<-- [start:pipeline]
def count_orders(events: list[dict], window: int) -> list[int]:
    """Returns the number of finished orders in each window, from the first window."""
    packed: Counter = Counter()
    for event in events:
        if "event_type" in event:
            packed[int(event["sim_ts"] // window)] += 1
    return [packed[number] for number in range(max(packed) + 1)]


def find_surges(events: list[dict], window: int = 10, factor: float = 2.5) -> list[int]:
    """Returns the start of each window with more than `factor` times the usual orders."""
    counts = count_orders(events, window)
    limit = factor * statistics.median(counts)
    starts = []
    for number, count in enumerate(counts):
        if count > limit:
            starts.append(number * window)
    return starts


# --8<-- [end:pipeline]


# --8<-- [start:score]
def score(found: list[int], labels: list[dict], within: int = 60) -> dict:
    """Returns how many planted faults the flagged windows caught and missed, and how
    many flagged windows had no fault before them."""
    faults = [label["sim_ts"] for label in labels]
    caught = 0
    for fault in faults:
        if any(fault <= start < fault + within for start in found):
            caught += 1
    false_alarms = 0
    for start in found:
        if not any(fault <= start < fault + within for fault in faults):
            false_alarms += 1
    return {
        "caught": caught,
        "missed": len(faults) - caught,
        "false": false_alarms,
    }


# --8<-- [end:score]
