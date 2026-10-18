"""Wait for one or all: a task's wait for the first or for every one of several branches.

`first(gap)` runs an order that waits for a restock signal or 30 seconds, with the
restock `gap` seconds later, and returns which branches were done. `two_at_once()`
fires two signals at the same instant. `all_with_a_failure()` waits for a signal and
a child flow, and the signal fails while the child still runs.
"""

import asyncio
import queue

from desflow import Entity, SimulationContext


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


def _shop():
    app = SimulationContext("Shop", factor=0)
    capture = Capture()
    app.add_egress(capture)
    return app, capture


# --8<-- [start:first]
def first(gap: float) -> dict:
    """Which branches were done when the order's wait ended."""
    app, capture = _shop()
    app.add_signal("restocked")
    seen = {}

    @app.task(
        wait={"any": {"stock": {"signal": "restocked"}, "limit": {"timeout": 30}}}
    )
    def order(task_id, wait):
        # `wait` holds each branch's done, ok and value
        seen.update({name: branch["done"] for name, branch in wait.items()})
        return {"event_type": "resumed"}

    @app.task(signal="restocked")
    def restock(task_id):
        return {"boxes": 6}

    def script(context):
        context.spawn(order(0))
        yield context.env.timeout(gap)
        context.spawn(restock(0))

    app.add_process(script)
    app.run(until=60)
    return seen


# --8<-- [end:first]


# --8<-- [start:tie]
def two_at_once() -> dict:
    """Two signals fire at the same instant: both branches are done."""
    app, capture = _shop()
    app.add_signal("boxes").add_signal("labels")
    seen = {}

    @app.task(
        wait={"any": {"boxes": {"signal": "boxes"}, "labels": {"signal": "labels"}}}
    )
    def order(task_id, wait):
        seen.update({name: branch["done"] for name, branch in wait.items()})
        return {"event_type": "resumed"}

    @app.task(signal="boxes")
    def deliver_boxes(task_id):
        return {"item": "boxes"}

    @app.task(signal="labels")
    def deliver_labels(task_id):
        return {"item": "labels"}

    def script(context):
        context.spawn(order(0))
        yield context.env.timeout(5)
        context.spawn(deliver_boxes(0))
        context.spawn(deliver_labels(0))

    app.add_process(script)
    app.run(until=10)
    return seen


# --8<-- [end:tie]


# --8<-- [start:all]
def all_with_a_failure() -> list:
    """(time, task, status) of the order and its inspection: the restock fails at 2,
    the order fails at once, and the inspection, still running, ends at 4."""
    app, capture = _shop()
    app.add_signal("restocked")
    app.add_service("inspecting", dist="constant", value=4)

    @app.task(
        wait={"all": {"stock": {"signal": "restocked"}, "check": {"task": "inspect"}}}
    )
    def order(task_id):
        return {"event_type": "resumed"}

    @app.task(service_id="inspecting")
    def inspect(task_id):
        return {"event_type": "inspected"}

    @app.task(
        signal="restocked", fail="attr.broken == 1", fail_reason="broken delivery"
    )
    def restock(task_id):
        return {"boxes": 6}

    def script(context):
        context.spawn(order(0))
        yield context.env.timeout(2)
        delivery = Entity("restocks-1", "restocks", {"broken": 1})
        context.spawn(restock(0, entity=delivery))

    app.add_process(script)
    app.run(until=10)
    return [
        (e["sim_ts"], e["task"], e["value"].get("status", "finished"))
        for e in capture.events
        if e["task"] in ("order", "inspect") and e["value"].get("status") != "started"
    ]


# --8<-- [end:all]

if __name__ == "__main__":
    print("restock after 10 s:", first(10))
    print("restock after 50 s:", first(50))
    print("two at once:", two_at_once())
    print("all, with a failure:", all_with_a_failure())
