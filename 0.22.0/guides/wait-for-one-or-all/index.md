# Wait for One or All

**Level:** Intermediate

Make a task wait for the first of several things, or for all of them, and read which happened. It builds on [Process Interaction](https://jaehyeon.me/desflow/latest/tutorials/process-interaction/index.md), which introduces signals and a wait with a limit, and on [Tasks and Processes](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/#wait-signals-and-child-flows), which lists every kind of branch.

______________________________________________________________________

## Wait for the First

`wait: {any: {...}}` ends when the first branch happens, as SimPy's `AnyOf` (`a | b`) does. Name each branch; after the wait, `wait.<branch>.done` says which happened. Here an order waits for a restock or 30 seconds, and a builder function that declares `wait` reads the branches:

```python
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

`first(10)` returns `{"stock": True, "limit": False}`: the restock came first. `first(50)` returns `{"stock": False, "limit": True}`. In YAML the same wait is

```yaml
dispatch:
  # The courier's departure, or 40 seconds, whichever comes first.
  wait:
    any:
      courier: {signal: courier_left}
      limit: {timeout: 40 s}
  payload: {event_type: dispatched}
  next:
    when: [{condition: wait.courier.done, task: by_courier}]
    default: by_post
```

and in the low-level API it is `yield env.any_of([restocked.wait(), env.timeout(30)])`.

When an `any` wait ends, its losing branches are cleaned up: a signal, timeout or condition branch is dropped, so it does nothing later. A losing child flow (`{task: name}`) keeps running to its end, as SimPy leaves a process running when nothing waits for it.

______________________________________________________________________

## Two at the Same Instant

Two branches can happen at the same simulation time. SimPy's `AnyOf` holds every event processed before the `AnyOf` itself, so both are in its value, and desflow reports both as done:

```python
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

`two_at_once()` returns `{"boxes": True, "labels": True}`. A model that routes on one branch should therefore check the branch it cares about, not assume that only one is done.

______________________________________________________________________

## Wait for All

`wait: {all: {...}}` ends when every branch has happened, as SimPy's `AllOf` (`a & b`) does. If a branch fails first, the wait fails at once, with that branch's reason, and the task publishes `failed`; branches still pending are cleaned up as for `any`, so a child flow runs on. Here the restock fails at 2 while the inspection, a child flow, runs until 4:

```python
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

`all_with_a_failure()` returns `[(0.0, "order", "queued"), (2.0, "order", "failed"), (4.0, "inspect", "finished")]`. The order's failure goes to its `on_fail` task when it has one, and otherwise ends its flow.

A wait never holds a resource. Waiting for all of several things that each free up a resource does not grant the resources together: the task asks for its resource only after the wait, and another task can take the unit in between. To hold several resources at once, list them on the task, `resource: [picker, bench]`, as [Resources](https://jaehyeon.me/desflow/latest/user-guide/resources/#several-resources) describes.

______________________________________________________________________

## Example

The `process-interaction` example waits for a child flow, then for the first of a courier's signal and a time limit. [Tasks and Processes](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/#example-process-interaction) shows how to download and run it.
