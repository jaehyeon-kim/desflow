"""SimPy's example files under docs/snippets/simpy/ print what SimPy ships beside them.

Each `<name>.py` is SimPy 4.1.2's `docs/examples/code/<name>.py` with SimPy's MIT
notice and the snippet markers added, and each `<name>.out` is SimPy's own output
file. Running the file with the locked SimPy and comparing what it prints with the
`.out` file shows that the code and output a page shows are SimPy's own.
"""

import asyncio
import queue
import subprocess
import sys
from collections import Counter
from functools import lru_cache
from pathlib import Path

import pytest

from desflow import SimulationContext
from desflow.connectors.egress.base import BaseEgress
from desflow.core.sinks import Sink

ROOT = Path(__file__).resolve().parents[2]
SIMPY = ROOT / "docs" / "snippets" / "simpy"
FILES = sorted(SIMPY.glob("*.py"))


def test_every_file_has_its_output():
    assert len(FILES) == 7
    assert sorted(p.stem for p in SIMPY.glob("*.out")) == [p.stem for p in FILES]


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.stem)
def test_simpy_file_prints_its_output(path):
    run = subprocess.run(
        [sys.executable, "-I", str(path)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
        check=True,
    )
    expected = path.with_suffix(".out").read_text(encoding="utf-8")
    assert run.stdout.replace("\r\n", "\n") == expected.replace("\r\n", "\n")


class _Capture(BaseEgress):
    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


@lru_cache(maxsize=None)
def _events(name):
    """The task events of a port's YAML blueprint, run unpaced as its page runs it."""
    app = SimulationContext.from_yaml(
        ROOT / "examples" / "yaml" / "simpy" / f"{name}.yaml"
    )
    app.factor = 0.0
    capture = _Capture()
    app._sinks.ingress = []
    app._sinks.egress = [Sink(capture)]
    app.run()
    return [r for r in capture.records if "key" in r]


def _kind(record):
    value = record["value"]
    return value.get("event_type") or value.get("status")


def _of(name, kind):
    return [r for r in _events(name) if _kind(r) == kind]


def _page(slug):
    return (ROOT / "docs" / "simpy" / "examples" / f"{slug}.md").read_text(
        encoding="utf-8"
    )


def _stated(slug, *figures):
    """Each figure is in the page's text."""
    text = " ".join(_page(slug).split())
    assert [f for f in figures if f not in text] == []


def test_latency_page_states_the_run():
    received = _of("latency", "received")
    first, last = received[0], received[-1]
    assert (first["sim_ts"], first["value"]["msg"]) == (15.0, "Sender sent this at 5")
    assert (last["sim_ts"], last["value"]["msg"]) == (95.0, "Sender sent this at 85")
    assert (len(_of("latency", "sent")), len(received)) == (19, 17)
    _stated(
        "latency",
        "Sender sent this at 5",
        "19 messages",
        "17",
        "95 seconds",
        "Sender sent this at 85",
    )


def test_bank_renege_page_states_the_run():
    reneged = [r["key"] for r in _of("bank_renege", "reneged")]
    assert reneged == ["Customer01", "Customer02", "Customer04"]
    finished = {r["key"]: round(r["sim_ts"], 3) for r in _of("bank_renege", "finished")}
    assert finished == {"Customer00": 24.981, "Customer03": 37.245}
    started = [r for r in _of("bank_renege", "started") if r["key"] == "Customer03"]
    assert round(started[0]["sim_ts"], 3) == 29.312
    _stated(
        "bank-renege",
        "`Customer01`, `Customer02` and `Customer04`",
        "24.981",
        "29.312",
        "37.245",
    )


def test_carwash_page_states_the_run():
    cars = [r for r in _of("carwash", "queued") if r["task"] == "car"]
    assert [(r["key"], r["sim_ts"]) for r in cars] == [
        ("Car 0", 0.0),
        ("Car 1", 0.0),
        ("Car 2", 0.0),
        ("Car 3", 0.0),
        ("Car 4", 5.0),
        ("Car 5", 14.0),
    ]
    washed = {r["key"]: r["sim_ts"] for r in _of("carwash", "washed")}
    assert washed == {
        "Car 0": 5.0,
        "Car 1": 5.0,
        "Car 2": 10.0,
        "Car 3": 10.0,
        "Car 4": 15.0,
        "Car 5": 19.0,
    }
    _stated(
        "carwash",
        "Six cars",
        "car 4 arrives at 5 seconds",
        "car 5 at 14 seconds",
        "car 5 at 19 seconds",
    )


def test_gas_station_page_states_the_run():
    assert len(_of("gas_station_refuel", "refueled")) == 5
    assert [r["sim_ts"] for r in _of("gas_station_refuel", "truck_called")] == [970.0]
    assert _of("gas_station_refuel", "truck_arrived") == []
    _stated(
        "gas-station-refuel", "Five cars", "970 seconds", "1270 seconds", "460 seconds"
    )


def test_machine_shop_page_states_the_run():
    parts = Counter(r["key"] for r in _of("machine_shop", "part_made"))
    assert sum(parts.values()) == 32637
    assert parts.most_common(1) == [("Machine 0", 3329)]
    assert min(parts.items(), key=lambda item: item[1]) == ("Machine 3", 3194)
    assert len(_of("machine_shop", "broken")) == 1124
    assert len(_of("machine_shop", "repaired")) == 1123
    assert len(_of("machine_shop", "other_done")) == 220
    assert len(_of("machine_shop", "preempted")) == 223
    _stated(
        "machine-shop",
        "32637 parts",
        "3329",
        "3194",
        "1124 machines",
        "1123 repairs",
        "220 other jobs",
        "223 times",
    )


def test_movie_renege_page_states_the_run():
    sold = {
        r["value"]["movie"]: round(r["sim_ts"], 3)
        for r in _of("movie_renege", "sold_out")
    }
    assert sold == {
        "Kill Process": 36.662,
        "Python Unchained": 39.662,
        "Pulp Implementation": 43.662,
    }
    left = Counter(r["value"]["movie"] for r in _of("movie_renege", "reneged"))
    assert left == {"Python Unchained": 17, "Kill Process": 8, "Pulp Implementation": 8}
    assert len(_of("movie_renege", "bought")) == 43
    assert len(_of("movie_renege", "balked")) == 154
    _stated(
        "movie-renege",
        "36.662",
        "39.662",
        "43.662",
        "17 moviegoers",
        "43 sales",
        "154 moviegoers",
    )


def test_process_communication_page_states_the_run():
    said = _of("process_communication", "said")
    received = _of("process_communication", "received")
    assert (len(said), len(received)) == (12, 12)
    assert len(_of("process_communication", "handled")) == 11
    late = [(r["value"]["text"], r["sim_ts"]) for r in received if r["value"]["late"]]
    assert late == [
        ("Generator A says hello at 38", 39.0),
        ("Generator A says hello at 63", 65.0),
    ]
    both = _of("process_communication_broadcast", "received")
    assert Counter(r["key"].split("-")[0] for r in both) == {
        "consumer_a": 12,
        "consumer_b": 12,
    }
    assert Counter(r["key"].split("-")[0] for r in both if r["value"]["late"]) == {
        "consumer_a": 2,
        "consumer_b": 4,
    }
    _stated(
        "process-communication",
        "12 messages",
        "38 and at 63 seconds",
        "39 and at 65 seconds",
        "11 of them",
        "6 receipts",
    )
