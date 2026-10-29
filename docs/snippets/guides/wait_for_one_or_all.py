"""Wait for one or all: a task's wait for the first or for every one of several branches.

`first(gap)` runs an order that waits for a restock signal or 30 seconds, with the
restock `gap` seconds later, and returns which branches were done. `two_at_once()`
fires two signals at the same instant. `all_with_a_failure()` waits for a signal and
a child flow, and the signal fails while the child still runs. `first_yaml`,
`two_at_once_yaml` and `all_with_a_failure_yaml` run the same with the blueprints in
docs/snippets/yaml, and wait_for_one_or_all_low_level.py with the low-level API.
`example_yaml` runs the example blueprint.

python wait_for_one_or_all.py, from the root of a clone, prints each result.
"""

import asyncio
import queue
import shutil
import sys
import tempfile
from pathlib import Path

from desflow import Entity, SimulationContext

BLUEPRINTS = Path("docs/snippets/yaml")  # from the root of a clone


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


def _events(task: str, capture: Capture) -> list:
    return [e for e in capture.events if e["task"] == task]


# --8<-- [start:yaml]
def run_blueprint(blueprint: Path, arrivals: dict) -> Capture:
    """Runs a blueprint with its arrivals replayed from CSV files beside it.

    `arrivals` maps each file to its rows, such as {"orders.csv": "time\\n0\\n"}.
    """
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        shutil.copy(blueprint, folder)
        shutil.copy(blueprint.parent / "wait_for_one_or_all_logic.py", folder)
        for file, rows in arrivals.items():
            (folder / file).write_text(rows)
        sys.path.insert(0, str(folder))  # the blueprint's !python payload
        try:
            app = SimulationContext.from_yaml(folder / blueprint.name)
            capture = Capture()
            app.add_egress(capture)
            app.run()  # until the blueprint's run.until
        finally:
            sys.path.remove(str(folder))
            sys.modules.pop("wait_for_one_or_all_logic", None)
    return capture


def first_yaml(gap: float, blueprint=BLUEPRINTS / "wait_first.yaml") -> dict:
    capture = run_blueprint(
        Path(blueprint),
        {"orders.csv": "time\n0\n", "restocks.csv": f"time\n{gap}\n"},
    )
    return _events("order", capture)[-1]["value"]["done"]


def two_at_once_yaml(blueprint=BLUEPRINTS / "wait_tie.yaml") -> dict:
    capture = run_blueprint(
        Path(blueprint),
        {
            "orders.csv": "time\n0\n",
            "boxes.csv": "time\n5\n",
            "labels.csv": "time\n5\n",
        },
    )
    return _events("order", capture)[-1]["value"]["done"]


def all_with_a_failure_yaml(blueprint=BLUEPRINTS / "wait_all.yaml") -> list:
    capture = run_blueprint(
        Path(blueprint),
        {"orders.csv": "time\n0\n", "restocks.csv": "time,broken\n2,1\n"},
    )
    return [
        (e["sim_ts"], e["task"], e["value"].get("status", "finished"))
        for e in capture.events
        if e["task"] in ("order", "inspect") and e["value"].get("status") != "started"
    ]


# --8<-- [end:yaml]


def example_yaml(path: str = "docs/snippets/yaml/wait_for_one_or_all_example.yaml"):
    """The example blueprint: the orders packed and refunded, and the restock times."""
    app = SimulationContext.from_yaml(path)
    capture = Capture()
    app.add_egress(capture)
    app.run()  # 10 minutes, from the blueprint
    readings = app.env.stats.readings()
    counts = {
        task: readings[f"Shop.stats.tasks.{task}.finished"]
        for task in ("pack", "refund")
    }
    restocks = _events("restock", capture)
    return {
        **counts,
        "restocked_at": [
            e["sim_ts"] for e in restocks if e["value"].get("event_type") == "restocked"
        ],
    }


if __name__ == "__main__":
    print("restock after 10 s:", first(10))
    print("restock after 50 s:", first(50))
    print("two at once:", two_at_once())
    print("all, with a failure:", all_with_a_failure())
    print("the same in YAML:", first_yaml(10), first_yaml(50))
    print(two_at_once_yaml(), all_with_a_failure_yaml())
    print("the example in YAML:", example_yaml())
