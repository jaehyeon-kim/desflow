"""The Serve Entities Together guide, builder API: seven orders arrive 1 second apart,
and an oven bakes them in batches of 3, or sooner when 2.5 seconds pass.

`batch_records(blueprint)` returns the (time, key, status) of each `started` and `baked`
record; with a path, it runs that blueprint, and without one, it builds the model, with
a batch that closes by an expression when `close_when` is true.
"""

import asyncio
import queue
import sys

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


def _app() -> SimulationContext:
    app = SimulationContext("Bakery", factor=0)
    app.add_resource("oven", current_cap=1)
    app.add_arrival("orders", "constant", value=1, max_events=7, spawn="bake")
    return app


def build() -> SimulationContext:
    app = _app()

    # --8<-- [start:batch]
    # The batch closes with 3 members, or 2.5 seconds after its first member
    @app.task(
        batch={"size": 3, "timeout": 2.5},
        resource_id="oven",  # one run of the oven serves the whole batch
        service_time=4,
    )
    def bake(task_id):
        return {"event_type": "baked"}

    # --8<-- [end:batch]
    return app


def build_close_when() -> SimulationContext:
    app = _app()

    # --8<-- [start:close]
    # Close with 2 members once the first has waited 2 seconds, or at 5 members
    @app.task(
        batch={"size": 5, "close_when": "batch.size >= 2 and batch.age >= 2"},
        resource_id="oven",
        service_time=4,
    )
    def bake(task_id):
        return {"event_type": "baked"}

    # --8<-- [end:close]
    return app


def batch_records(blueprint: str = "", close_when: bool = False) -> list:
    if blueprint:
        app = SimulationContext.from_yaml(blueprint)
    else:
        app = build_close_when() if close_when else build()
    capture = Capture()
    app.add_egress(capture)
    app.run(until=30)
    return sorted(
        (e["sim_ts"], e["key"], e["value"].get("status", "baked"))
        for e in capture.events
        if e["value"].get("status", "baked") in ("started", "baked")
    )


if __name__ == "__main__":
    for row in batch_records(close_when="close_when" in sys.argv[1:]):
        print("check:", row)
