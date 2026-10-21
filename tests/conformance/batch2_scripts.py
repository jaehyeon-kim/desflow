"""Process helpers for the Batch 2 conformance blueprints.

A blueprint names these with `!python` to start a task once at a set time, which a
builder arrival loop does in Python. They keep the YAML models small and let a
waiter and a firing task start at exactly chosen instants.
"""

from typing import Any


def _start_at(context: Any, moment: float, task: str):
    """Starts one instance of `task` at `moment`, as a new entity of its own."""

    def run():
        yield context.env.timeout(moment - context.env.now)
        wrapper = context._tasks[task]
        context.spawn(wrapper(context.next_instance(task), context))

    return run


def one_shot_model(context: Any):
    """early waits before the firing, late starts after it; close fires closing."""
    context.spawn(_start_at(context, 1, "early")())
    context.spawn(_start_at(context, 2, "close")())
    context.spawn(_start_at(context, 5, "late")())
    yield context.env.event()


def on_fire_model(context: Any):
    """watcher waits for ready; measure fires it from its own finished payload."""
    context.spawn(_start_at(context, 0, "watcher")())
    context.spawn(_start_at(context, 1, "measure")())
    yield context.env.event()


def one_start(context: Any):
    """Starts one `start` task at 1, as a single arrival would."""
    yield from _start_at(context, 1, "start")()
    yield context.env.event()


def one_driver(context: Any):
    """Starts one `driver` task at 0, as a single arrival would."""
    yield from _start_at(context, 0, "driver")()
    yield context.env.event()


def start_task(context: Any, task: str, at: float = 0.0):
    """Starts one instance of `task` at `at`, as a single arrival would."""
    yield from _start_at(context, at, task)()
    yield context.env.event()


def two_charges(context: Any):
    """Starts two charge instances and a driver on one car entity, so the driver's
    running wait selects several instances."""
    entity = context._new_entity("car", None)
    charge, driver = context._tasks["charge"], context._tasks["driver"]
    context.spawn(
        charge(context.next_instance("charge"), *context._task_args, entity=entity)
    )
    context.spawn(
        charge(context.next_instance("charge"), *context._task_args, entity=entity)
    )
    context.spawn(
        driver(context.next_instance("driver"), *context._task_args, entity=entity)
    )
    yield context.env.event()
