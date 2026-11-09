"""Splits the docs tests between parallel CI jobs.

`DOCS_SHARD=2/6` keeps the second of six groups, dealt by the times in
`durations.json` as tests/sharding.py sets out.

`DOCS_STORE_DURATIONS=1 pytest tests/docs -n auto` records the times again. Record
them when tests are added or become much slower, so the groups stay even.
"""

import json
import os
from collections import defaultdict
from pathlib import Path

from tests.sharding import keep_share

DURATIONS = Path(__file__).parent / "durations.json"

_measured: dict = defaultdict(float)


def pytest_collection_modifyitems(config, items):
    shard = os.environ.get("DOCS_SHARD")
    if shard:
        keep_share(config, items, shard, DURATIONS)


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
