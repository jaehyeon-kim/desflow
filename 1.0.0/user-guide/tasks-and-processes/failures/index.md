# Failures

This page shows when a [task](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/tasks/index.md) fails and what happens next.

______________________________________________________________________

## What It Is

A failure is a task that does not finish its work: an order that fails its check, a wait that fails, code that raises an error. desflow records it, then sends the entity to a task that handles it, or ends the entity's flow.

A task fails in these cases:

- Its `fail` condition holds after its service. `fail` reads the task's own payload as `payload.<field>`, as `next` does.
- The wait it needs fails, or the child flow it waits for fails.
- A step raises an error, or an expression cannot be evaluated: a `service_time`, a `payload` field, a `gen: expr`. A builder task's function that raises an exception fails it too.

______________________________________________________________________

## How to Use It

### Declare a Failure

A task declares when it fails with `fail`, why with `fail_reason`, and where the entity goes with `on_fail`. `fail_reason` is text, a number or a generator read when the task fails. A reason of `None`, such as a generator that returns nothing, falls back to the text of the `fail` condition, and an empty text or 0 stays as the reason. An error in the generator fails the task, with the error's type and place as the reason. The task that `on_fail` names reads the failure as `upstream.status` (`failed`) and `upstream.reason`. `on_fail` needs no `fail`: a failed wait, a failed child flow, a step that raises and an expression that cannot be evaluated go to it too.

```yaml
pack:
  service: packing
  resource: packers
  # The task fails when this condition holds after its service
  fail: t > 12
  # The reason is text, a number or a generator
  fail_reason: after the cut-off
  # Where the order goes after a failure; it reads upstream.status and upstream.reason
  on_fail: refund
  payload: {event_type: packed}
refund:
  payload:
    event_type: refunded
    why: {gen: expr, expr: upstream.reason}
```

```yaml
check:
  # The wait fails when the child flow fails; on_fail needs no fail condition
  wait: {task: inspect}
  on_fail: hold
  payload: {event_type: checked}
inspect:
  service: packing
  fail: t > 12
  fail_reason: failed inspection
  payload: {event_type: inspected}
hold:
  payload:
    event_type: held
    why: {gen: expr, expr: upstream.reason}
```

`@app.task(fail="t > 12", fail_reason="after the cut-off", on_fail="refund")` takes the same keys, and keeps an empty or 0 reason as YAML does. The Builder tab of [Handle a Failure Nobody Handles](#handle-a-failure-nobody-handles) runs a task that fails with no `on_fail`.

A process catches the exception of the child it yields, as SimPy code does: `try: yield env.process(inspect())` with `except Exception as error:`. It then starts what `on_fail` would start.

### Handle a Failure Nobody Handles

`run.on_unhandled_failure` sets what happens to a failure of a task, child flow or one-shot signal that nobody handles. It has two values:

- `stop`, the default in YAML, the builder and the low-level API: the failure stops the run, as plain SimPy does. A root task that fails with no `on_fail` raises an exception, and so does a one-shot signal failed with nobody waiting. The exception reaches the caller of `run()`.
- `continue`: the failure is marked as handled (SimPy calls this defused), and the run goes on. A data generator should not stop on one failed order.

The builder and YAML switch to `continue` with `run(on_unhandled_failure="continue")` or `run: {on_unhandled_failure: continue}`, and the low-level API with `DynamicRealtimeEnvironment(on_unhandled_failure="continue")`. In the low-level API, an exception raised in a process always reaches the process that yields it, and one nobody handles stops the run, as in SimPy. One signal failure can be defused alone with `defused` ([Signals and Waiting](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#actions-when-an-event-happens)).

Under `run`:

```yaml
# stop, the default, raises a failure nobody handles to the caller of run();
# continue defuses it and the run goes on
on_unhandled_failure: continue
```

Under `stop`, the failure reaches the caller of `run()` as an exception. `TaskFailed` names the task and the reason, and this prints `pack failed: after the cut-off`:

```python
# Under stop, the first failure that no on_fail handles reaches the caller of run()
try:
    shop().run(until=60, on_unhandled_failure="stop")
except TaskFailed as failure:
    print(f"{failure.task} failed: {failure.reason}")
```

`stop` is the default. `DynamicRealtimeEnvironment(on_unhandled_failure="continue")` switches to `continue`.

A run that stops on a signal raises `SignalFailed` when the signal fails, as [Run Length](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#stop-early) describes:

`run.stop` names the signal. When the signal fails, `desflow run` reports the reason and exits with an error.

This prints `closing failed: the till did not balance`:

```python
# A run that stops on a signal raises the reason the signal failed with. The packing
# failures are left to continue, so only the signal ends the run
try:
    shop().run(until=60, on_unhandled_failure="continue", stop={"signal": "closing"})
except SignalFailed as failure:
    print(f"{failure.signal} failed: {failure.reason}")
```

`env.run(until=event)` on the signal's event raises the exception the event failed with, as in SimPy.

______________________________________________________________________

## How It Works

A failed task then does three things, in order:

1. It publishes `failed` instead of its finished payload, with a `reason`: the condition's text, or its `fail_reason`.
1. It fails its `signal`, so every task waiting for the signal fails too.
1. It goes to its `on_fail` task. Without `on_fail`, the entity's flow ends, and a child flow fails the task waiting for it, with the same reason. A failure nobody handles then follows `on_unhandled_failure`.

At the `basic` statistics level and above, each failure is counted as `tasks.<task>.failed`, and each flow that ends in one as `flows.<arrival>.failed`. The `inspect` task of the example in [Signals and Waiting](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows) fails a gift and sends it to be wrapped.
