"""Deals the collected tests into shares for parallel CI jobs.

Both `TEST_SHARD` (tests/conftest.py, with `tests/durations.json`) and `DOCS_SHARD`
(tests/docs/conftest.py, with `tests/docs/durations.json`) use it. `2/6` keeps the
second of six shares. The tests are dealt slowest first, each to the share with the
least time so far, by the recorded times. A test not listed counts as the time under
the key `__other__` when the file has one, otherwise as the median time.
Every share collects the same tests, so each test runs in exactly one of them.
"""

import json
import statistics
from pathlib import Path
from typing import Dict, Iterable, List

# The key of the time a test not listed counts as
OTHER = "__other__"


def deal(nodeids: Iterable[str], count: int, times: Dict[str, float]) -> Dict[str, int]:
    """The share, from 0, of each test id."""
    times = dict(times)
    default = times.pop(OTHER, None)
    if default is None:
        default = statistics.median(times.values()) if times else 1.0
    totals = [0.0] * count
    share_of = {}
    for nodeid in sorted(nodeids, key=lambda n: (-times.get(n, default), n)):
        share = totals.index(min(totals))
        share_of[nodeid] = share
        totals[share] += times.get(nodeid, default)
    return share_of


def keep_share(config, items: List, shard: str, durations: Path) -> None:
    """Keeps the items of share `shard` (such as `2/6`) and deselects the rest."""
    index, count = (int(part) for part in shard.split("/"))
    times = json.loads(durations.read_text()) if durations.exists() else {}
    share_of = deal([item.nodeid for item in items], count, times)
    keep = [item for item in items if share_of[item.nodeid] == index - 1]
    kept = {id(item) for item in keep}
    config.hook.pytest_deselected(items=[i for i in items if id(i) not in kept])
    items[:] = keep
