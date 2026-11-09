"""The Model Breakdowns guide: a station that fails after 5 seconds of work.

`down_periods_yaml(clock)` runs docs/snippets/yaml/breakdowns_busy.yaml, with its clock
set to `clock`, and `down_periods(clock)` builds the same model. Both return the
(time, status) of each record the station publishes.
"""

import asyncio
import queue
import tempfile
from pathlib import Path

from desflow import SimulationContext


class Capture:
    """An egress that keeps every event it receives."""

    def __init__(self):
        self.events = []

    async def run(self, egress_queue):
        try:
            while True:
                try:
                    batch = egress_queue.get_nowait()
                    self.events += [r for r in batch if r["stream_type"] == "event"]
                except queue.Empty:
                    await asyncio.sleep(0.005)
        except asyncio.CancelledError:
            pass


def _station_periods(app: SimulationContext, capture: Capture) -> list:
    app.run(until=30)
    return [
        (e["sim_ts"], e["value"]["status"])
        for e in capture.events
        if e["key"] == "station"
    ]


def down_periods(clock: str = "busy") -> list:
    """(time, status) of the station's failures and repairs."""
    app = SimulationContext("Shop", factor=0)
    capture = Capture()
    app.add_egress(capture)
    app.add_service("packing", dist="constant", value=8)
    app.add_arrival(
        "orders", dist="constant", value=10, first_at=10, max_events=1, spawn="pack"
    )
    app.add_resource("station", current_cap=1)
    # --8<-- [start:busy]
    app.add_breakdown(
        "station",
        time_to_failure={"dist": "constant", "value": 5},
        repair={"dist": "constant", "value": 2},
        work="finish",
        # Count the time to failure only while a unit is in use. The default,
        # "calendar", counts it from the start or the last repair, busy or idle.
        clock=clock,
    )
    # --8<-- [end:busy]

    @app.task(service_id="packing", resource_id="station")
    def pack(task_id):
        return {"event_type": "packed"}

    return _station_periods(app, capture)


def down_periods_yaml(clock: str = "busy", path: str = "breakdowns_busy.yaml") -> list:
    text = (
        Path(path).read_text(encoding="utf-8").replace("clock: busy", f"clock: {clock}")
    )
    with tempfile.TemporaryDirectory() as tmp:
        copy = Path(tmp) / "breakdowns.yaml"
        copy.write_text(text, encoding="utf-8")
        app = SimulationContext.from_yaml(copy)
        capture = Capture()
        app.add_egress(capture)
        return _station_periods(app, capture)


if __name__ == "__main__":
    print("check: busy clock:", down_periods("busy"))
    print("check: calendar clock:", down_periods("calendar"))
