# Failures

This page shows when a [task](index.md) fails and what happens next.

---

## How to Use It

A task fails when its `fail` condition holds after its service, when the wait it needs fails, or when its function raises: a builder task's function, or a YAML task's `!python` payload function. `fail` reads the task's own payload as `payload.<field>`, as `next` does. A failed task publishes `failed` with a `reason`, the condition's text or its `fail_reason`, instead of its finished payload; it fails its `signal`, so every waiter of the signal fails too; and it goes to its `on_fail` task. Without `on_fail` the entity's flow ends, and a child flow fails the task waiting for it, with the same reason. Each failure is counted as `tasks.<task>.failed`, and a flow that ends in one as `flows.<arrival>.failed`, at `basic` and above. The `inspect` task of the example above fails a gift and sends it to be wrapped.

What a failure nobody handles does is set by `run.on_unhandled_failure`. Under `continue`, the default, a task, child flow or one-shot signal failure that nobody handles is defused and the run goes on, because a data generator should not stop on one failed order. Under `stop` such a failure stops the run, as plain SimPy does: a root task failing with no `on_fail` raises, a one-shot signal failed with nobody waiting raises, and the exception reaches the caller of `run()`. The builder and YAML take it as `run(on_unhandled_failure="stop")` or `run: {on_unhandled_failure: stop}`; the low-level API as `DynamicRealtimeEnvironment(on_unhandled_failure="stop")`. In the low-level API an exception raised in a process always reaches the process that yields it, and one nobody handles stops the run, as SimPy.
