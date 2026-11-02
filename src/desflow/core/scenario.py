"""Scenarios: registry changes and ground-truth records at set simulation times.

`run_scenario` is a SimPy process any API can start. Each step applies at its time
on the simulation clock, so a scenario repeats exactly and works at `factor: 0`. A
step is scheduled as an urgent event, as SimPy schedules its own `until`, so it
applies before every other model event at that time: an arrival gap drawn at that
moment already uses a new rate. A step at or after the run's `until` never applies.
"""

from typing import Any, Callable, Iterable, List, Mapping, Optional

from simpy.events import URGENT, Event

from desflow.utils import suggestion, time_to_seconds
from desflow.values.models import has_generator

STEP_KEYS = ("at", "path", "value", "record", "interrupt")


def check_step(step: Mapping[str, Any]) -> dict:
    """
    Checks one scenario step and returns it with `at` in seconds.

    Args:
        step: A mapping with `at` and a `path` and `value`, a `record`, an
            `interrupt`, or more than one.

    Returns:
        dict: The step, with `at` in seconds.

    Raises:
        ValueError: If a key is unknown, `at` is missing or negative, a path has no
            value, a value has no path, the step does nothing, or an interrupt names
            no task (a `handle` is the interrupt key of a task, not of a step).
    """
    unknown = set(step) - set(STEP_KEYS)
    if unknown:
        raise ValueError(
            f"a scenario step does not take {', '.join(sorted(unknown))}"
            + (
                suggestion(sorted(unknown)[0], STEP_KEYS)
                or f"; it takes {', '.join(STEP_KEYS)}"
            )
        )
    if "at" not in step:
        raise ValueError("a scenario step needs at")
    at = step["at"]
    seconds = time_to_seconds(at) if isinstance(at, str) else at
    if seconds < 0:
        raise ValueError(f"a scenario step's at must be 0 or more, got {at!r}")
    if ("path" in step) != ("value" in step):
        raise ValueError("a scenario step's path and value go together")
    if not ({"path", "record", "interrupt"} & set(step)):
        raise ValueError(
            "a scenario step needs a path and value, a record or an interrupt"
        )
    action = step.get("interrupt")
    if action is not None and (
        not isinstance(action, Mapping) or action.get("task") is None
    ):
        raise ValueError(
            "a scenario step interrupts a task; a handle is the interrupt key of "
            "a task, which knows the names it opened"
            if isinstance(action, Mapping) and action.get("handle") is not None
            else "a scenario step's interrupt names the task to interrupt"
        )
    return {**step, "at": seconds}


def run_scenario(
    env: Any,
    steps: Iterable[Mapping[str, Any]],
    until: Optional[float] = None,
    interrupt: Optional[Callable[..., Any]] = None,
    evaluate: Optional[Callable[[Any], Any]] = None,
):
    """
    Applies each step at its time, in time order and, at equal times, in the order
    given. A step's `at` counts from when this process starts, the start of the run
    when it is started with the model, so it does not depend on `initial_time`.

    A step with a `path` and `value` changes the registry, and each `on_change`
    callback receives the change with the source `"scenario"`; one with a `record`
    publishes a ground-truth record of the change (`env.publish_label`); one with an
    `interrupt`, `{task, where, cause}`, calls `interrupt(task, where=..., cause=...)`,
    which the builder and YAML pass as `context.interrupt`.

    Args:
        env: The environment.
        steps: The steps, each a mapping as `check_step` takes.
        until: The run's end, a reading of the clock; steps due at or after it are
            dropped, as an event due at exactly `until` does not run.
        interrupt: What an `interrupt` step calls; such steps need it.
        evaluate: Evaluates a value that holds a generator when its step is due;
            the builder and YAML pass the run's value runtime. A `value` is
            evaluated before the change, a `record` after it, so a `param` in the
            record reads the new value.

    Raises:
        ParamRejected: When the run reaches a step whose value the registry refuses,
            such as an exponential rate of 0. It stops the run, as any exception in
            a process does. The builder and a blueprint check the values with the
            run's overrides before the clock starts, so their runs reach it only
            when an ingress message, an `on:` action or a process has changed a
            related value before the step.
    """
    ordered: List[dict] = sorted(
        (check_step(step) for step in steps), key=lambda step: step["at"]
    )
    # `at` counts from the start of the run, the moment this process starts, so a
    # run with an initial_time of 8 h applies a step at 30 min at 8.5 h
    origin = env.now
    for step in ordered:
        at = origin + step["at"]
        if until is not None and at >= until:
            return
        if at > env.now:
            yield _urgent_timeout(env, at - env.now)
        apply_step(
            env,
            step.get("path"),
            step.get("value"),
            record=step.get("record"),
            interrupt=interrupt,
            action=step.get("interrupt"),
            source="scenario",
            refuse=True,
            evaluate=evaluate,
        )


def apply_step(
    env: Any,
    path: Optional[str],
    value: Any = None,
    record: Optional[Mapping[str, Any]] = None,
    interrupt: Optional[Callable[..., Any]] = None,
    action: Optional[Mapping[str, Any]] = None,
    source: Optional[str] = None,
    refuse: bool = False,
    evaluate: Optional[Callable[[Any], Any]] = None,
) -> None:
    """Applies one change: a registry `path` and `value`, a ground-truth `record`,
    and an `interrupt` action. Shared by `run_scenario` and an `on:` list's actions,
    so the registry changes by one path.

    Args:
        env: The environment, for the registry and `publish_label`.
        path: A registry path to set, or None.
        value: The value to set at `path`.
        record: A ground-truth record to publish, or None.
        interrupt: The function an `action` calls, as `run_scenario` takes it.
        action: An `{task, where, cause}` interrupt, or None.
        source: Where the registry change came from, as `ParamChange.source` records
            it; `run_scenario` passes `"scenario"`, and a model's own `on:` action
            passes none.
        refuse: Raise `ParamRejected` for a value the registry refuses, instead of
            logging it and going on. `run_scenario` passes True, so a refused step
            stops the run at its time; an `on:` action keeps the default.
        evaluate: Evaluates a `value`, `record` or `cause` that holds a generator,
            or None when there is none to evaluate.

    Raises:
        ParamRejected: If `refuse` is True and the registry refuses the value.
    """
    old = new = None
    if path is not None:
        if evaluate is not None and has_generator(value):
            value = evaluate(value)
        old = env.registry.get(path).value
        if refuse:
            env.registry.change(path, value, source=source)
        else:
            env.registry.update(path, value, source=source)
        new = env.registry.get(path).value
    if record is not None:
        if evaluate is not None and has_generator(record):
            record = evaluate(record)
        env.publish_label(record, path=path, old=old, new=new)
    if action is not None:
        if interrupt is None:
            raise ValueError("an interrupt step needs an interrupt function")
        cause = action.get("cause")
        if evaluate is not None and has_generator(cause):
            cause = evaluate(cause)
        interrupt(action["task"], where=action.get("where"), cause=cause)


def _urgent_timeout(env: Any, delay: float) -> Event:
    """A timeout scheduled urgent, so it runs before normal events at its time."""
    event = Event(env)
    event._ok, event._value = True, None
    env.schedule(event, URGENT, delay)
    return event
