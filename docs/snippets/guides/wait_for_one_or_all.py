"""Wait for one or all: a task's wait for the first or for every one of several branches.

`first(gap)` runs an order that waits for a restock signal or 30 seconds, with the
restock `gap` seconds later, and returns which branches were done. `two_at_once()`
fires two signals at the same instant. `all_with_a_failure()` waits for a signal and
a child flow, and the signal fails while the child still runs. `first_yaml`,
`two_at_once_yaml` and `all_with_a_failure_yaml` run the same with the blueprints in
docs/snippets/yaml, and wait_for_one_or_all_low_level.py with the low-level API.
`example_yaml` runs the example blueprint.

python wait_for_one_or_all.py prints each result on a line that starts with
"check: ". Run it in a folder that holds the blueprints wait_first.yaml, wait_tie.yaml
and wait_all.yaml (docs/snippets/yaml/).
"""

# ruff: noqa: E402

# --8<-- [start:header]
import asyncio
import queue
import shutil
import tempfile
from pathlib import Path


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


def _events(task: str, capture: Capture) -> list:
    return [e for e in capture.events if e["task"] == task]


# --8<-- [end:header]

from desflow import Entity, SimulationContext


# --8<-- [start:first]
def _shop():
    app = SimulationContext("Shop", factor=0)
    capture = Capture()
    app.add_egress(capture)
    return app, capture


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
    app.run(until=10, on_unhandled_failure="continue")
    return [
        (e["sim_ts"], e["task"], e["value"].get("status", "finished"))
        for e in capture.events
        if e["task"] in ("order", "inspect") and e["value"].get("status") != "started"
    ]


# --8<-- [end:all]


# --8<-- [start:suspend]
def suspend_and_resume() -> list:
    """(time, value) of each order a truck woke: it wakes the two oldest of three."""
    app, capture = _shop()
    woken = []

    @app.task(wait={"suspend": True})
    def hold(task_id, wait):
        # wait["suspend"]["value"] is the value of the resume that woke the order
        woken.append((float(app.env.now), wait["suspend"]["value"]))
        return {"event_type": "released"}

    @app.task(resume={"task": "hold", "limit": 2, "value": "truck-1"})
    def unload(task_id):
        return {"event_type": "unloaded"}

    def script(context):
        for number in range(3):
            yield context.env.timeout(1)
            context.spawn(hold(number))
        yield context.env.timeout(2)
        context.spawn(unload(0))

    app.add_process(script)
    app.run(until=10)
    return woken


# --8<-- [end:suspend]


# --8<-- [start:each]
def each_of_three() -> tuple:
    """(time, flags) of an order that packs three boxes, of 2, 5 and 9 seconds, and
    goes on when two are packed."""
    app, capture = _shop()
    seen = {}

    @app.task(
        wait={
            "all": {
                "boxes": {
                    "each": [2, 5, 9],
                    "group": {"at_least": 2},
                    "branch": {"timeout": {"after": "each.value"}},
                }
            }
        }
    )
    def order(task_id, wait):
        seen["done"] = (float(app.env.now), wait["boxes"]["flags"])
        return {"event_type": "packed"}

    def script(context):
        context.spawn(order(0))
        yield context.env.timeout(0)

    app.add_process(script)
    app.run(until=30)
    return seen["done"]


# --8<-- [end:each]


# --8<-- [start:pool]
def pool_of_requests() -> tuple:
    """(time, granted, time of the next order) of an order that asks for three packers
    at once, packs with the first it gets and cancels the other two requests. A second
    order arrives at 1 s and asks for a packer."""
    from desflow.values import Template

    app, capture = _shop()
    app.add_resource("packers", current_cap=1)
    packed = Template(
        {
            "event_type": "packed",
            "granted": {"gen": "expr", "expr": "handle.pool.flags"},
        },
        name="tasks.order",
    )

    @app.task(
        open={"pool": {"each": 3, "branch": {"request": "packers"}}},
        wait={"handle": "pool", "index": 0},
        service_time=2,
        cancel="pool",  # the requests not yet granted: the second and the third
        release={"handle": "pool", "index": 0},
    )
    def order(task_id):
        return packed

    @app.task(resource_id="packers", service_time=1)
    def late_order(task_id):
        return {"event_type": "late_packed"}

    def script(context):
        context.spawn(order(0))
        yield context.env.timeout(1)
        context.spawn(late_order(0))

    app.add_process(script)
    app.run(until=30)
    return _pool_result(capture)


# --8<-- [end:pool]


# --8<-- [start:yaml]
def run_blueprint(blueprint: Path, arrivals: dict) -> Capture:
    """Runs a blueprint with its arrivals replayed from CSV files beside it.

    `arrivals` maps each file to its rows, such as {"orders.csv": "time\\n0\\n"}.
    """
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        shutil.copy(blueprint, folder)
        for file, rows in arrivals.items():
            (folder / file).write_text(rows)
        app = SimulationContext.from_yaml(folder / blueprint.name)
        capture = Capture()
        app.add_egress(capture)
        app.run()  # until the blueprint's run.until
    return capture


def first_yaml(gap: float, blueprint="wait_first.yaml") -> dict:
    capture = run_blueprint(
        Path(blueprint),
        {"orders.csv": "time\n0\n", "restocks.csv": f"time\n{gap}\n"},
    )
    return _events("order", capture)[-1]["value"]["done"]


def two_at_once_yaml(blueprint="wait_tie.yaml") -> dict:
    capture = run_blueprint(
        Path(blueprint),
        {
            "orders.csv": "time\n0\n",
            "boxes.csv": "time\n5\n",
            "labels.csv": "time\n5\n",
        },
    )
    return _events("order", capture)[-1]["value"]["done"]


def all_with_a_failure_yaml(blueprint="wait_all.yaml") -> list:
    capture = run_blueprint(
        Path(blueprint),
        {"orders.csv": "time\n0\n", "restocks.csv": "time,broken\n2,1\n"},
    )
    return [
        (e["sim_ts"], e["task"], e["value"].get("status", "finished"))
        for e in capture.events
        if e["task"] in ("order", "inspect") and e["value"].get("status") != "started"
    ]


def suspend_and_resume_yaml(blueprint="wait_suspend.yaml") -> list:
    capture = run_blueprint(Path(blueprint), {})
    return [
        (e["sim_ts"], e["value"]["by"])
        for e in _events("hold", capture)
        if e["value"].get("event_type") == "released"
    ]


def each_of_three_yaml(blueprint="wait_each.yaml") -> tuple:
    packed = _events("order", run_blueprint(Path(blueprint), {}))[-1]
    return packed["sim_ts"], packed["value"]["flags"]


def _pool_result(capture: Capture) -> tuple:
    """(time the order is packed, its granted flags, time the next order is packed),
    with None for a record that never comes."""
    packed = [e for e in _events("order", capture) if "granted" in e["value"]]
    late = [e for e in _events("late_order", capture) if "event_type" in e["value"]]
    return (
        packed[0]["sim_ts"],
        packed[0]["value"]["granted"],
        late[0]["sim_ts"] if late else None,
    )


def pool_of_requests_yaml(blueprint="wait_pool.yaml") -> tuple:
    return _pool_result(run_blueprint(Path(blueprint), {}))


# --8<-- [end:yaml]


def example_yaml(path: str = "wait_for_one_or_all_example.yaml"):
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


# --8<-- [start:main]
if __name__ == "__main__":
    print("check: restock after 10 s:", first(10))
    print("check: restock after 50 s:", first(50))
    print("check: two at once:", two_at_once())
    print("check: all, with a failure:", all_with_a_failure())
    print("check: the same in YAML:", first_yaml(10), first_yaml(50))
    print("check: two at once in YAML:", two_at_once_yaml())
    print("check: all, with a failure, in YAML:", all_with_a_failure_yaml())
    print("check: suspend and resume:", suspend_and_resume())
    print("check: suspend and resume, in YAML:", suspend_and_resume_yaml())
    print("check: two of three boxes:", each_of_three())
    print("check: two of three boxes, in YAML:", each_of_three_yaml())
    print("check: the first of three packers:", pool_of_requests())
    print("check: the first of three packers, in YAML:", pool_of_requests_yaml())
# --8<-- [end:main]
