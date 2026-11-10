"""The Builder API page: the arguments it shows for the keys of a blueprint.

The page says what each run prints. These tests run the two snippet files and read the
records they publish.
"""

import ast
import logging
import runpy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PAGES = ROOT / "docs" / "snippets" / "pages"


def _records(caplog) -> list[dict]:
    """The records ConsoleEgress logged, one dictionary for each [EVT] line."""
    found = []
    for message in caplog.messages:
        for line in message.splitlines():
            if "[EVT] " in line:
                found.append(ast.literal_eval(line.split("[EVT] ", 1)[1]))
    return found


def test_arrival_arguments_start_four_keyed_orders(caplog):
    """The arrival gap is 30 s, the first is at 5 s, and four orders start."""
    caplog.set_level(logging.INFO)
    runpy.run_path(str(PAGES / "builder_arguments.py"), run_name="__main__")
    records = _records(caplog)

    starts = [r for r in records if r["task"] == "pack_order"]
    assert sorted({r["key"] for r in starts}) == [f"order-{n}" for n in range(4)]
    assert min(r["sim_ts"] for r in starts) == 5.0
    assert not [
        r for r in records if r["task"] == "record" and "event_type" in r["value"]
    ]


def test_holding_arguments_make_the_third_order_give_up(caplog):
    """The third order waits 15 s for the bench, leaves, and goes to gave_up."""
    caplog.set_level(logging.INFO)
    runpy.run_path(str(PAGES / "builder_holding.py"), run_name="__main__")
    records = _records(caplog)

    reneged = [r for r in records if r["value"].get("status") == "reneged"]
    assert [(r["key"], r["sim_ts"]) for r in reneged] == [("orders-3", 45.0)]
    left = [r for r in records if r["task"] == "gave_up"]
    assert [r["value"] for r in left] == [{"event_type": "left", "key": "orders-3"}]
