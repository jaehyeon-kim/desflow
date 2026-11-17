# Wait for One or All

Make a task wait for the first of several things, or for all of them, and read which happened. It builds on [Process Interaction](https://jaehyeon.me/desflow/latest/tutorials/process-interaction/index.md), which introduces signals and a wait with a limit, and on [Signals and Waiting](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows), which lists every kind of branch.

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

A wait for signals, tasks and times never holds a resource. Waiting for all of several things that each free up a resource does not grant the resources together: the task asks for its resource only after the wait, and another task can take the unit in between. To hold several resources at once, list them on the task, `resource: [picker, bench]`, as [Several Resources](https://jaehyeon.me/desflow/latest/user-guide/resources/several-resources/#several-resources) describes.

## Nest Any and All

A branch of an `any` or an `all` may itself be an `any` or an `all`, so a wait nests to any depth. For example, `wait: {all: {look: {task: inspect}, either: {any: {stock: {signal: restocked}, limit: {timeout: 30 s}}}}}` is done when the inspection ends and either a restock or a time limit arrives. Each branch name in the tree is unique, and `wait.<name>` reads any node, a group or a leaf: `wait.either.done` is true once the inner `any` is done, and `wait.either.value` maps its done leaves to their values. The low-level form is `yield (stock | limit) & inspect`.

## At Least Some of Them

`wait: {at_least: 2, of: {a: ..., b: ..., c: ...}}` is done when at least two of its branches are, as SimPy's `Condition` with a count rule. `at_least` is a whole number of 1 or more, and an `at_least` above the number of branches is refused when the file loads. An empty `of` succeeds at once, whatever the count. The low-level form is `Condition(env, lambda events, count: count >= 2, [a, b, c])`.

## Wait for a Unit, an Item or Room

A branch may also be a resource `request`, a store or container `get`, or a `put`, so a task waits for the first of a unit, an item, a signal or a time. A `request` branch that wins holds its unit through the task's service and gives it back when the task ends, as a listed `resource` does; `wait.<branch>.value` is the request. A `get` branch takes an item, and `wait.<branch>.value` is that item; a `put` branch adds the payload of the task before it, or `None` for the first task of a flow. An `any` ends when its first branch is granted, and every request, get or put granted at that same instant is kept as won too. A branch that loses the race is cancelled, so it takes or holds nothing: `wait: {any: {unit: {request: packers}, limit: {timeout: 30 s}}}` waits for a packer or a time limit, and gives the packer back if the limit comes first. A single branch is named after its resource for a `request`, and `get` or `put` otherwise. An interrupt with `on_interrupt: restart` during the wait starts the branches again, so an item a get already took is used by neither the old nor the new branch, as SimPy cannot put a taken item back.

Requests in a wait count in the [order of acquisition](https://jaehyeon.me/desflow/latest/user-guide/resources/several-resources/#several-resources): they come before the task's `resource` list. The requests under an `all`, or under an `at_least` of 2 or more, are made together, and the first one granted is held while the others are waited for, so no other task may acquire two of them one after the other, in either order. A single `request` branch of an `any` ends the `any` when it is granted, so it holds nothing while the `any` waits, and such branches set no order. A group branch of an `any`, such as an `all` of two requests, can hold a unit before it is won, so its requests are made together with those of the other branches. A task that keeps a resource for a next task that requests it in its wait is refused when the model is built, because the request would wait behind the unit kept for it.

## An Empty Wait

An `any` or an `all` with no branches is accepted, and succeeds at once with an empty value. A task whose wait is `{any: {}}` or `{all: {}}` goes on in the step it starts, as SimPy's `env.any_of([])` and `env.all_of([])` do. With a `patience` of 0 the wait still wins: the empty wait succeeds the instant it is built, so it is already processed when the task resumes, and the task does not renege.

______________________________________________________________________

## Example

The `process-interaction` example waits for a child flow, then for the first of a courier's signal and a time limit. [Processes](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/processes/#example-process-interaction) shows how to download and run it.
