"""Reneging and balking: ties between a grant and a patience, and an entity balance.

`tie(waiter_at, patience)` runs one packer held from 0 to 6 seconds and one waiting
order, and returns what the order did at 6. `balance(path)` runs a blueprint and
checks that every order that arrived is packed, reneged, balked or still waiting.
"""

import asyncio
import queue
from collections import Counter

from desflow import SimulationContext


class Capture:
    """An egress that keeps every record it receives."""

    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        try:
            while True:
                try:
                    self.records.extend(egress_queue.get_nowait())
                except queue.Empty:
                    await asyncio.sleep(0.005)
        except asyncio.CancelledError:
            pass


# --8<-- [start:tie]
def tie(waiter_at: float, patience: float) -> str:
    """The outcome of an order whose patience ends when the packer comes free."""
    app = SimulationContext("Shop", factor=0)
    app.add_resource("packers", current_cap=1, max_cap=1)
    app.add_service("hold", dist="constant", value=6)
    app.add_service("packing", dist="constant", value=1)
    capture = Capture()
    app.add_egress(capture)

    @app.task(service_id="hold", resource_id="packers")
    def holder(task_id):
        return {"event_type": "held"}

    @app.task(service_id="packing", resource_id="packers", patience=patience)
    def pack(task_id):
        return {"event_type": "order_packed"}

    def script(context):
        context.spawn(holder(0))  # takes the packer at 0 and keeps it until 6
        if waiter_at:
            yield context.env.timeout(waiter_at)
        context.spawn(pack(0))
        yield context.env.timeout(0)

    app.add_process(script)
    app.run(until=8)
    statuses = [
        r["value"].get("status")
        for r in capture.records
        if r["stream_type"] == "event" and r["task"] == "pack"
    ]
    return "reneged" if "reneged" in statuses else "served"


# --8<-- [end:tie]


# --8<-- [start:balance]
def balance(path: str) -> Counter:
    """Every order that arrived, by what became of it."""
    app = SimulationContext.from_yaml(path)
    capture = Capture()
    app.add_egress(capture)
    app.run()
    last = {}
    for record in capture.records:
        if record["stream_type"] == "event":
            value = record["value"]
            last[record["key"]] = value.get("status", value.get("event_type"))
    outcome = Counter(last.values())
    # queued or started: still in the shop when the run ended
    stats = app.env.stats.readings()
    name = "Shop.stats.tasks.pack.{}".format
    assert outcome["order_packed"] == stats[name("finished")]
    assert outcome["reneged"] == stats.get(name("reneged"), 0)
    assert outcome["balked"] == stats.get(name("balked"), 0)
    return outcome


# --8<-- [end:balance]

if __name__ == "__main__":
    print("arrives at 3, patience 3:", tie(3, 3))
    print("arrives at 0, patience 6:", tie(0, 6))
    print(dict(balance("examples/yaml/patience.yaml")))
