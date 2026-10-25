# Failures

This page shows when a [task](index.md) fails and what happens next.

---

## How to Use It

A task fails in three cases:

- Its `fail` condition holds after its service. `fail` reads the task's own payload as `payload.<field>`, as `next` does.
- The wait it needs fails.
- Its function raises an exception. The function is a builder task's function or a YAML task's `!python` payload function.

A failed task then does three things, in order:

1. It publishes `failed` instead of its finished payload, with a `reason`: the condition's text, or its `fail_reason`.
2. It fails its `signal`, so every task waiting for the signal fails too.
3. It goes to its `on_fail` task. Without `on_fail`, the entity's flow ends, and a child flow fails the task waiting for it, with the same reason.

At the `basic` statistics level and above, each failure is counted as `tasks.<task>.failed`, and each flow that ends in one as `flows.<arrival>.failed`. The `inspect` task of the example in [Signals and Waiting](signals-and-waiting.md#wait-signals-and-child-flows) fails a gift and sends it to be wrapped.

`run.on_unhandled_failure` sets what happens to a failure of a task, child flow or one-shot signal that nobody handles. It has two values:

- `continue`, the default in YAML and the builder: the failure is marked as handled (SimPy calls this defused), and the run goes on. A data generator should not stop on one failed order.
- `stop`, the default in the low-level API: the failure stops the run, as plain SimPy does. A root task that fails with no `on_fail` raises an exception, and so does a one-shot signal failed with nobody waiting. The exception reaches the caller of `run()`.

The builder and YAML switch to `stop` with `run(on_unhandled_failure="stop")` or `run: {on_unhandled_failure: stop}`, and the low-level API switches to `continue` with `DynamicRealtimeEnvironment(on_unhandled_failure="continue")`. In the low-level API, an exception raised in a process always reaches the process that yields it, and one nobody handles stops the run, as in SimPy.

In a blueprint, under `run`:

```yaml title="docs/snippets/yaml/task_endings.yaml:unhandled"
# continue, the default: a failure nobody handles is defused and the run goes on;
# stop raises it to the caller of run()
on_unhandled_failure: continue
```

In Python, the failure reaches the caller of `run()` as an exception. `TaskFailed` names the task and the reason, and this prints `pack failed: after the cut-off`:

```python title="docs/snippets/pages/failures.py:task-failed"
# Under stop, the first failure that no on_fail handles reaches the caller of run()
try:
    shop().run(until=60, on_unhandled_failure="stop")
except TaskFailed as failure:
    print(f"{failure.task} failed: {failure.reason}")
```

A run that stops on a signal raises `SignalFailed` when the signal fails, as [Run Length](../time-and-pacing/run-length.md#stop-early) describes. This prints `closing failed: the till did not balance`:

```python title="docs/snippets/pages/failures.py:signal-failed"
# A run that stops on a signal raises the reason the signal failed with
try:
    shop().run(until=60, stop={"signal": "closing"})
except SignalFailed as failure:
    print(f"{failure.signal} failed: {failure.reason}")
```

---

## Stability and Limits

The limits of this page are listed with those of the whole section, in [Stability and Limits](index.md#stability-and-limits) on the Tasks and Processes page.
