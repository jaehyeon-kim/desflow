# Wait for One or All

Make a task wait for the first of several things, or for all of them, and read which happened.

---

## Before You Start

- [Process Interaction](../../learn/shop-logic/process-interaction.md) introduces signals and a wait with a limit.
- [Signals and Waiting](../../user-guide/tasks-and-processes/signals-and-waiting.md#wait-signals-and-child-flows) lists every kind of branch.

---

## Steps

The Builder and YAML blocks in the steps are functions of one script, `wait_for_one_or_all.py`, in the order the blocks appear. Its start, which the functions share, is below. The line `from desflow import Entity, SimulationContext` follows it, and `_shop` is the first function of the first Builder block:

```python title="docs/snippets/guides/wait_for_one_or_all.py:header"
import asyncio
import queue
import shutil
import sys
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
```

The YAML blocks read their files from the folder of the script. Save each blueprint under the name in its title, `wait_first.yaml`, `wait_tie.yaml` and `wait_all.yaml`, and the payload function as `wait_for_one_or_all_logic.py`. The Low-level blocks are functions of a second script, `wait_for_one_or_all_low_level.py`, which [Check the Result](#check-the-result) completes.

### Wait for the First

`wait: {any: {...}}` ends when the first branch happens, as SimPy's `AnyOf` (`a | b`) does. Name each branch. After the wait, `wait.<branch>.done` says which happened. Here an order waits for a restock or 30 seconds. A builder function that declares `wait` reads the branches, and so does a blueprint's `!python` payload:

=== "YAML"

    ```yaml title="docs/snippets/yaml/wait_first.yaml:first"
    simulation:
      sim_id: Shop
      factor: 0

    signals: [restocked]

    arrivals:
      orders: {replay: {path: orders.csv}, spawn: order}
      restocks: {replay: {path: restocks.csv}, spawn: restock}

    tasks:
      order:
        wait:
          any:
            stock: {signal: restocked}
            limit: {timeout: 30}
        # Called as branches(task_id, context, wait); its event says which branches were done
        payload: !python wait_for_one_or_all_logic.branches
      restock:
        signal: restocked
        payload: {boxes: 6}

    run:
      until: 60
    ```

    The payload function reads the wait:

    ```python title="docs/snippets/yaml/wait_for_one_or_all_logic.py"
    """Python for the Wait for One or All blueprints: the order's payload, which reads
    its wait, as the builder's task function does."""


    def branches(task_id, context, wait):
        """The order's event: which branches were done when its wait ended."""
        # `wait` holds each branch's done, ok and value
        return {
            "event_type": "resumed",
            "done": {name: branch["done"] for name, branch in wait.items()},
        }
    ```

    `first_yaml(gap)` replays the order at 0 and the restock at `gap` from CSV files:

    ```python title="docs/snippets/guides/wait_for_one_or_all.py:yaml"
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
    ```

=== "Builder"

    ```python title="docs/snippets/guides/wait_for_one_or_all.py:first"
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
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/wait_for_one_or_all_low_level.py:first"
    def first(gap: float) -> dict:
        """Which branches were done when the order's wait ended."""
        env = DynamicRealtimeEnvironment(factor=0)
        restocked = Signal(env, "restocked")
        seen = {}

        def order():
            branches = {"stock": restocked.wait(), "limit": env.timeout(30)}
            done = yield env.any_of(list(branches.values()))
            # The value of AnyOf holds the events that had happened
            seen.update({name: event in done for name, event in branches.items()})

        def restock():
            yield env.timeout(gap)
            restocked.fire({"boxes": 6})

        env.process(order())
        env.process(restock())
        env.run(until=60)
        env.teardown()
        return seen
    ```

With the restock at 10 seconds, every form returns `{"stock": True, "limit": False}`: the restock came first. With the restock at 50 seconds, it returns `{"stock": False, "limit": True}`. To route on the branch that happened, give the task a `next` with `condition: wait.stock.done`, as the [example](#check-the-result) does.

When an `any` wait ends, its losing branches are cleaned up: a signal, timeout or condition branch is dropped, so it does nothing later. A losing child flow (`{task: name}`) keeps running to its end, as SimPy leaves a process running when nothing waits for it.

### Two at the Same Instant

Two branches can happen at the same simulation time. SimPy's `AnyOf` holds every event processed before the `AnyOf` itself, so both are in its value, and desflow reports both as done:

=== "YAML"

    ```yaml title="docs/snippets/yaml/wait_tie.yaml:tie"
    simulation:
      sim_id: Shop
      factor: 0

    signals: [boxes, labels]

    arrivals:
      orders: {replay: {path: orders.csv}, spawn: order}
      box_deliveries: {replay: {path: boxes.csv}, spawn: deliver_boxes}
      label_deliveries: {replay: {path: labels.csv}, spawn: deliver_labels}

    tasks:
      order:
        wait:
          any:
            boxes: {signal: boxes}
            labels: {signal: labels}
        payload: !python wait_for_one_or_all_logic.branches
      deliver_boxes:
        signal: boxes
        payload: {item: boxes}
      deliver_labels:
        signal: labels
        payload: {item: labels}

    run:
      until: 10
    ```

    `two_at_once_yaml()` runs it with `run_blueprint`, as `first_yaml` does.

=== "Builder"

    ```python title="docs/snippets/guides/wait_for_one_or_all.py:tie"
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
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/wait_for_one_or_all_low_level.py:tie"
    def two_at_once() -> dict:
        """Two signals fire at the same instant: both branches are done."""
        env = DynamicRealtimeEnvironment(factor=0)
        boxes, labels = Signal(env, "boxes"), Signal(env, "labels")
        seen = {}

        def order():
            branches = {"boxes": boxes.wait(), "labels": labels.wait()}
            done = yield env.any_of(list(branches.values()))
            seen.update({name: event in done for name, event in branches.items()})

        def deliver():
            yield env.timeout(5)
            boxes.fire({"item": "boxes"})
            labels.fire({"item": "labels"})

        env.process(order())
        env.process(deliver())
        env.run(until=10)
        env.teardown()
        return seen
    ```

Every form returns `{"boxes": True, "labels": True}`. A model that routes on one branch should therefore check that branch, not assume that only one is done.

### Wait for All

`wait: {all: {...}}` ends when every branch has happened, as SimPy's `AllOf` (`a & b`) does. If a branch fails first, the wait fails at once, with that branch's reason, and the task publishes `failed`. Branches still pending are cleaned up as for `any`, so a child flow runs on. Here the restock fails at 2 while the inspection, a child flow, runs until 4:

=== "YAML"

    ```yaml title="docs/snippets/yaml/wait_all.yaml:all"
    simulation:
      sim_id: Shop
      factor: 0

    signals: [restocked]

    services:
      inspecting: {dist: constant, value: 4}

    arrivals:
      orders: {replay: {path: orders.csv}, spawn: order}
      restocks: {replay: {path: restocks.csv}, spawn: restock}

    tasks:
      order:
        wait:
          all:
            stock: {signal: restocked}
            check: {task: inspect}
        payload: {event_type: resumed}
      inspect:
        service: inspecting
        payload: {event_type: inspected}
      restock:
        signal: restocked
        fail: attr.broken == 1  # from the broken column of restocks.csv
        fail_reason: broken delivery
        payload: {boxes: 6}

    run:
      until: 10
    ```

    `all_with_a_failure_yaml()` runs it with `run_blueprint`, with the restock at 2 marked broken in its CSV file.

=== "Builder"

    ```python title="docs/snippets/guides/wait_for_one_or_all.py:all"
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
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/wait_for_one_or_all_low_level.py:all"
    def all_with_a_failure() -> list:
        """(time, task, status) of the order and its inspection: the restock fails at 2,
        the order fails at once, and the inspection, still running, ends at 4."""
        env = DynamicRealtimeEnvironment(factor=0)
        restocked = Signal(env, "restocked")
        seen = []

        def inspect():
            yield env.timeout(4)
            seen.append((env.now, "inspect", "finished"))

        def order():
            seen.append((env.now, "order", "queued"))
            try:
                yield env.all_of([restocked.wait(), env.process(inspect())])
            except SignalFailed:
                # A failed branch fails the AllOf at once; the inspection runs on
                seen.append((env.now, "order", "failed"))

        def restock():
            yield env.timeout(2)
            restocked.fail("broken delivery")

        env.process(order())
        env.process(restock())
        env.run(until=10)
        env.teardown()
        return seen
    ```

Every form returns `[(0.0, "order", "queued"), (2.0, "order", "failed"), (4.0, "inspect", "finished")]`. The order's failure goes to its `on_fail` task when it has one, and otherwise ends its flow. In the low-level form, the failed signal raises `SignalFailed` in the order.

A wait for signals, tasks and times never holds a resource. Waiting for all of several things that each free up a resource does not grant the resources together: the task asks for its resource only after the wait, and another task can take the unit in between. To hold several resources at once, list them on the task, `resource: [picker, bench]`, as [Several Resources](../../user-guide/resources/several-resources.md#several-resources) describes.

### Nest Any and All

A branch of an `any` or an `all` may itself be an `any` or an `all`, so a wait nests to any depth. This wait is done when the invoice is printed and either two boxes are taken or 5 seconds pass:

=== "YAML"

    ```yaml title="docs/snippets/yaml/wait_groups.yaml:nested"
    # Go on once the invoice is printed and either two boxes are taken or 5 seconds pass
    wait:
      all:
        invoice: {timeout: 4 s}
        either:
          any:
            boxes: {get: {container: boxes, amount: 2}}
            limit: {timeout: 5 s}
    ```

=== "Builder"

    `@app.task(wait=...)` takes the same mapping as a dict.

=== "Low-level"

    The low-level form is `yield (boxes | limit) & invoice`.

Each branch name in the tree is unique. `wait.<name>` reads any node, whether a group or a leaf. For example, `wait.either.done` is true once the inner `any` is done, and `wait.either.value` maps its done leaves to their values.

### At Least Some of Them

`wait: {at_least: 2, of: {a: ..., b: ..., c: ...}}` is done when at least two of its branches are done. SimPy's `Condition` with a count rule does the same. `at_least` is a whole number of 1 or more, and an `at_least` above the number of branches is refused when the file loads. An empty `of` succeeds at once, whatever the count.

In a blueprint, an order waits for both its label and its invoice:

=== "YAML"

    ```yaml title="docs/snippets/yaml/wait_groups.yaml:all"
    # Go on once both the label and the invoice are printed
    wait:
      all:
        label: {timeout: 2 s}
        invoice: {timeout: 4 s}
    ```

=== "Builder"

    `@app.task(wait=...)` takes the same mapping as a dict.

=== "Low-level"

    The low-level form is `yield env.all_of([label, invoice])`, SimPy's `AllOf`.

It then waits for two of three checks:

=== "YAML"

    ```yaml title="docs/snippets/yaml/wait_groups.yaml:at-least"
    # Go on once two of the three checks have answered
    wait:
      at_least: 2
      of:
        stock: {timeout: 1 s}
        payment: {timeout: 3 s}
        address: {timeout: 6 s}
    ```

=== "Builder"

    `@app.task(wait=...)` takes the same mapping as a dict.

=== "Low-level"

    The low-level form is `Condition(env, lambda events, count: count >= 2, [a, b, c])`.

### Wait for a Unit, an Item or Room

A branch may also be a resource `request`, a store or container `get`, or a `put`. A task can then wait for the first of a unit, an item, a signal or a time. A branch of each kind does this when it wins:

- A `request` holds its unit through the task's service and gives it back when the task ends, as a listed `resource` does. Its `wait.<branch>.value` is the request.
- A `get` takes an item, and `wait.<branch>.value` is that item.
- A `put` adds the payload of the task before it, or `None` for the first task of a flow.

A single branch is named after its resource for a `request`, and `get` or `put` otherwise. An `any` ends when its first branch is granted, and every request, get or put granted at that same instant is kept as won too. A branch that loses is cancelled, so it takes or holds nothing. An interrupt with `on_interrupt: restart` during the wait starts the branches again. An item a get already took is then used by neither the old nor the new branch, because SimPy cannot put a taken item back.

This wait is for a packer or a time limit, and gives up the packer request if the limit comes first:

=== "YAML"

    ```yaml title="docs/snippets/yaml/wait_groups.yaml:request"
    # A packer, or give up after 30 seconds; a packer won is held through packing
    wait:
      any:
        unit: {request: packers}
        limit: {timeout: 30 s}
    ```

=== "Builder"

    `@app.task(wait=...)` takes the same mapping as a dict.

=== "Low-level"

    The low-level form is `yield unit | env.timeout(30)`, as [Add Reneging and Balking](reneging-and-balking.md#leave-after-a-wait-and-refuse-to-join) does.

A `get` branch with a time limit, whose `value` says why the wait ended:

=== "YAML"

    ```yaml title="docs/snippets/yaml/wait_groups.yaml:get"
    # Two boxes, or go on without them after 5 seconds with the value "no boxes"
    wait:
      any:
        boxes: {get: {container: boxes, amount: 2}}
        limit: {timeout: {after: 5 s, value: no boxes}}
    ```

=== "Builder"

    `@app.task(wait=...)` takes the same mapping as a dict.

=== "Low-level"

    The low-level form is `yield boxes.get(2) | env.timeout(5)`.

A `put` branch on its own:

=== "YAML"

    ```yaml title="docs/snippets/yaml/wait_groups.yaml:put"
    # Put the closed order's payload on the shelf, waiting while the shelf is full
    wait: {put: {store: shelf}}
    ```

=== "Builder"

    `@app.task(wait=...)` takes the same mapping as a dict.

=== "Low-level"

    The low-level form is `yield shelf.put(item)`.

Requests in a wait count in the [order of acquisition](../../user-guide/resources/several-resources.md#several-resources): they come before the task's `resource` list. How a wait makes its requests depends on the group:

- Under an `all`, or an `at_least` of 2 or more, the requests are made together. The first one granted is held while the others are waited for, so no other task can acquire two of them one after the other, in either order.
- A single `request` branch of an `any` holds nothing while the `any` waits, and ends the `any` when it is granted. So such branches set no order.
- A group branch of an `any`, such as an `all` of two requests, can hold a unit before the `any` is won. So its requests are made together with those of the other branches.

A task that keeps a resource for a next task that requests it in its wait is refused when the model is built, because the request would wait behind the unit kept for it.

### An Empty Wait

An `any` or an `all` with no branches is accepted, and succeeds at once with an empty value. A task whose wait is `{any: {}}` or `{all: {}}` goes on in the step it starts, as SimPy's `env.any_of([])` and `env.all_of([])` do. With a `patience` of 0 the wait still wins. The empty wait succeeds the instant it is built, so it is already processed when the task resumes. The task therefore does not renege.

---

## Check the Result

The script prints the result of every step above. The Builder and YAML functions end the script with:

```python title="docs/snippets/guides/wait_for_one_or_all.py:main"
if __name__ == "__main__":
    print("check: restock after 10 s:", first(10))
    print("check: restock after 50 s:", first(50))
    print("check: two at once:", two_at_once())
    print("check: all, with a failure:", all_with_a_failure())
    print("check: the same in YAML:", first_yaml(10), first_yaml(50))
    print("check: two at once in YAML:", two_at_once_yaml())
    print("check: all, with a failure, in YAML:", all_with_a_failure_yaml())
```

Run it in the folder with the script, the three blueprints and the payload file, and keep the lines that start with `check:`:

```bash
# With uv
uv run --no-project --with desflow wait_for_one_or_all.py | grep "^check:"
# With pip
pip install desflow
python wait_for_one_or_all.py | grep "^check:"
```

```text
check: restock after 10 s: {'stock': True, 'limit': False}
check: restock after 50 s: {'stock': False, 'limit': True}
check: two at once: {'boxes': True, 'labels': True}
check: all, with a failure: [(0.0, 'order', 'queued'), (2.0, 'order', 'failed'), (4.0, 'inspect', 'finished')]
check: the same in YAML: {'stock': True, 'limit': False} {'stock': False, 'limit': True}
check: two at once in YAML: {'boxes': True, 'labels': True}
check: all, with a failure, in YAML: [(0.0, 'order', 'queued'), (2.0, 'order', 'failed'), (4.0, 'inspect', 'finished')]
```

The Low-level script, `wait_for_one_or_all_low_level.py`, starts with the line `from desflow import DynamicRealtimeEnvironment, Signal, SignalFailed`, followed by the three Low-level functions in the order they appear above. It ends with:

```python title="docs/snippets/guides/wait_for_one_or_all_low_level.py:main"
if __name__ == "__main__":
    print("restock after 10 s:", first(10))
    print("restock after 50 s:", first(50))
    print("two at once:", two_at_once())
    print("all, with a failure:", all_with_a_failure())
```

It needs no blueprint. Run it:

```bash
# With uv
uv run --no-project --with desflow wait_for_one_or_all_low_level.py
# With pip
pip install desflow
python wait_for_one_or_all_low_level.py
```

```text
restock after 10 s: {'stock': True, 'limit': False}
restock after 50 s: {'stock': False, 'limit': True}
two at once: {'boxes': True, 'labels': True}
all, with a failure: [(0, 'order', 'queued'), (2, 'order', 'failed'), (4, 'inspect', 'finished')]
```

The low-level form prints the times of the last line without a decimal point, because its timeouts are whole numbers.

The example is a shop where each order waits for a restock or 30 seconds, whichever comes first. A restock waits for both its delivery, which takes 10 seconds, and a stock count, which takes 20 seconds. Orders arrive about every 10 seconds, and a delivery every minute. Its files are in [Full Files](#full-files). Save the form you use in a folder of its own, and run it:

=== "YAML"

    The records go to the terminal as JSON lines. Keep the last count of each task and the time of each restock:

    ```bash
    desflow run wait_for_one_or_all_example.yaml --stdout > records.jsonl
    grep -oE '"path_id":"Shop.stats.tasks.(pack|refund).finished","value":[0-9]+' records.jsonl | tail -n 2
    grep '"event_type":"restocked"' records.jsonl | grep -oE '"sim_ts":[0-9.]+'
    ```

    ```text
    "path_id":"Shop.stats.tasks.refund.finished","value":35
    "path_id":"Shop.stats.tasks.pack.finished","value":27
    "sim_ts":80.0
    "sim_ts":140.0
    "sim_ts":200.0
    "sim_ts":260.0
    "sim_ts":320.0
    "sim_ts":380.0
    "sim_ts":440.0
    "sim_ts":500.0
    "sim_ts":560.0
    ```

=== "Builder"

    ```bash
    uv run --no-project --with desflow wait_for_one_or_all_example.py
    ```

    ```text
    {'pack': 27, 'refund': 35, 'restocked_at': [80.0, 140.0, 200.0, 260.0, 320.0, 380.0, 440.0, 500.0, 560.0]}
    ```

=== "Low-level"

    ```bash
    uv run --no-project --with desflow wait_for_one_or_all_example_low_level.py
    ```

    ```text
    {'pack': 27, 'refund': 35, 'restocked_at': [80, 140, 200, 260, 320, 380, 440, 500, 560]}
    ```

Each form runs once for 10 simulation minutes at `factor=0`: 27 orders are packed after a restock and 35 are refunded at the time limit. The restocks come at 80, 140 and so on to 560 seconds. Each comes 20 seconds after its delivery starts, because the wait for all ends with the stock count, the longer of its two branches. With `any` in place of `all`, each restock would come after the 10-second delivery instead, and more orders would be packed.

The `process-interaction` example waits for a child flow, then for the first of a courier's signal and a time limit. [Processes](../../user-guide/tasks-and-processes/processes.md#example-process-interaction) shows how to download and run it.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/yaml/wait_for_one_or_all_example.yaml"
        # The Wait for One or All guide: a shop where orders wait for a restock or a time
        # limit, as wait_for_one_or_all_example.py builds it. Run it with:
        # desflow run wait_for_one_or_all_example.yaml
        simulation:
          sim_id: Shop
          factor: 0
          random_seed: 42

        signals: [restocked]

        arrivals:
          orders: {dist: exponential, rate: 0.1, spawn: order}
          deliveries: {dist: constant, value: 60, spawn: restock}

        services:
          counting: {dist: constant, value: 20}

        tasks:
          # Each order waits for the next restock or 30 seconds, whichever comes first
          order:
            wait:
              any:
                stock: {signal: restocked}
                limit: {timeout: 30}
            payload: {event_type: waited}
            next:
              when: [{condition: wait.stock.done, task: pack}]
              default: refund
          pack:
            payload: {event_type: packed}
          refund:
            payload: {event_type: refunded}
          # A restock waits for its delivery, 10 seconds, and a stock count, a child flow
          # of 20 seconds; when both are done it finishes and fires restocked
          restock:
            wait:
              all:
                delivery: {timeout: 10}
                count: {task: count_stock}
            signal: restocked
            payload: {event_type: restocked}
          count_stock:
            service: counting
            payload: {event_type: counted}

        stats: basic

        run:
          until: 10 min
        ```

    === "Builder"

        ```python title="docs/snippets/guides/wait_for_one_or_all_example.py"
        """Wait for one or all: a shop where orders wait for a restock or a time limit.

        Each order waits for the next restock or 30 seconds, whichever comes first, and is
        packed after a restock or refunded at the limit. A restock waits for both its
        delivery and a stock count, then fires the signal the orders wait for. The script
        runs 10 minutes at factor 0 and prints how many orders were packed and refunded, and
        the times of the restocks: python wait_for_one_or_all_example.py.
        """

        from desflow import SimulationContext

        app = (
            SimulationContext(sim_id="Shop", factor=0, random_seed=42)
            .add_arrival("orders", dist="exponential", rate=0.1, spawn="order")
            .add_arrival("deliveries", dist="constant", value=60, spawn="restock")
            .add_service("counting", dist="constant", value=20)
            .add_signal("restocked")
            .with_stats("basic")
        )


        # Each order waits for the next restock or 30 seconds, whichever comes first
        @app.task(
            wait={"any": {"stock": {"signal": "restocked"}, "limit": {"timeout": 30}}},
            next={
                "when": [{"condition": "wait.stock.done", "task": "pack"}],
                "default": "refund",
            },
        )
        def order(task_id):
            return {"event_type": "waited"}


        @app.task()
        def pack(task_id):
            return {"event_type": "packed"}


        @app.task()
        def refund(task_id):
            return {"event_type": "refunded"}


        restocked_at = []  # the time of each restock


        # A restock waits for its delivery, 10 seconds, and a stock count, a child flow of
        # 20 seconds; when both are done it finishes and fires restocked
        @app.task(
            wait={"all": {"delivery": {"timeout": 10}, "count": {"task": "count_stock"}}},
            signal="restocked",
        )
        def restock(task_id):
            restocked_at.append(app.env.now)
            return {"event_type": "restocked"}


        @app.task(service_id="counting")
        def count_stock(task_id):
            return {"event_type": "counted"}


        def outcome() -> dict:
            """Runs 10 minutes and returns the orders packed and refunded, and the times of
            the restocks."""
            app.run(until="10 min")
            readings = app.env.stats.readings()
            counts = {
                task: readings[f"Shop.stats.tasks.{task}.finished"]
                for task in ("pack", "refund")
            }
            return {**counts, "restocked_at": restocked_at}


        if __name__ == "__main__":
            print(outcome())
        ```

    === "Low-level"

        ```python title="docs/snippets/guides/wait_for_one_or_all_example_low_level.py"
        """Wait for one or all, low-level API: a shop where orders wait for a restock or a
        time limit.

        The low-level version of wait_for_one_or_all_example.py. Each order waits for the
        next restock or 30 seconds, whichever comes first, and is packed after a restock or
        refunded at the limit. A restock waits for both its delivery and a stock count, then
        fires the signal the orders wait for. The script runs 10 minutes at factor 0 and
        prints how many orders were packed and refunded, and the times of the restocks:
        python wait_for_one_or_all_example_low_level.py.
        """

        import itertools

        from desflow import (
            DistributionConfig,
            DynamicRealtimeEnvironment,
            Sampler,
            SimParameter,
            Signal,
        )

        PARAMS = SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="exponential", rate=0.1)},
        )


        def outcome() -> dict:
            """Runs 10 minutes and returns the orders packed and refunded, and the times of
            the restocks."""
            env = DynamicRealtimeEnvironment(factor=0)
            env.registry.register_sim_parameter(PARAMS)
            restocked = Signal(env, "restocked")
            counts = {"pack": 0, "refund": 0}
            restocked_at = []  # the time of each restock

            # Each order waits for the next restock or 30 seconds, whichever comes first
            def order():
                stock = restocked.wait()
                done = yield env.any_of([stock, env.timeout(30)])
                counts["pack" if stock in done else "refund"] += 1

            def count_stock():
                yield env.timeout(20)

            # A restock waits for its delivery, 10 seconds, and a stock count, a child
            # process of 20 seconds; when both are done it fires restocked
            def restock():
                yield env.all_of([env.timeout(10), env.process(count_stock())])
                restocked_at.append(env.now)
                restocked.fire()

            def orders(sampler):
                gap = env.registry.get_config("Shop.arrival.orders")
                while True:
                    yield env.timeout(sampler.sample(gap))
                    env.process(order())

            def deliveries():
                for _ in itertools.count():
                    yield env.timeout(60)
                    env.process(restock())

            env.process(orders(Sampler(seed=42)))
            env.process(deliveries())
            env.run(until=600)
            env.teardown()
            return {**counts, "restocked_at": restocked_at}


        if __name__ == "__main__":
            print(outcome())
        ```
