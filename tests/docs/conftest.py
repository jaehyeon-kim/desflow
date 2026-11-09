"""Splits the docs tests between parallel CI jobs.

`DOCS_SHARD=2/6` keeps the second of six groups. The tests are dealt slowest first,
each to the group with the least time so far, by the times in `durations.json`; a
test not listed there counts as the median time. Every group collects the same tests,
so each test runs in exactly one of them.

`DOCS_STORE_DURATIONS=1 pytest tests/docs -n auto` records the times again. Record
them when tests are added or become much slower, so the groups stay even.
"""

import json
import os
import statistics
from collections import defaultdict
from pathlib import Path

DURATIONS = Path(__file__).parent / "durations.json"

_measured: dict = defaultdict(float)


def _groups(nodeids, count):
    times = json.loads(DURATIONS.read_text()) if DURATIONS.exists() else {}
    default = statistics.median(times.values()) if times else 1.0
    totals = [0.0] * count
    group_of = {}
    for nodeid in sorted(nodeids, key=lambda n: (-times.get(n, default), n)):
        group = totals.index(min(totals))
        group_of[nodeid] = group
        totals[group] += times.get(nodeid, default)
    return group_of


def pytest_collection_modifyitems(config, items):
    shard = os.environ.get("DOCS_SHARD")
    if not shard:
        return
    index, count = (int(part) for part in shard.split("/"))
    group_of = _groups([item.nodeid for item in items], count)
    keep = [item for item in items if group_of[item.nodeid] == index - 1]
    config.hook.pytest_deselected(items=[item for item in items if item not in keep])
    items[:] = keep


def pytest_runtest_logreport(report):
    if os.environ.get("DOCS_STORE_DURATIONS"):
        _measured[report.nodeid] += report.duration


def pytest_sessionfinish(session):
    # Under pytest-xdist the reports reach the controller, which has no workerinput
    if os.environ.get("DOCS_STORE_DURATIONS") and not hasattr(
        session.config, "workerinput"
    ):
        times = {nodeid: round(t, 3) for nodeid, t in sorted(_measured.items())}
        DURATIONS.write_text(json.dumps(times, indent=0) + "\n")
