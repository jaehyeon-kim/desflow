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
            value, a value has no path, or the step does nothing.
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
    seconds = time_to_seconds(at) if isinstance(at, str) else float(at)
    if seconds < 0:
        raise ValueError(f"a scenario step's at must be 0 or more, got {at!r}")
    if ("path" in step) != ("value" in step):
        raise ValueError("a scenario step's path and value go together")
    if not ({"path", "record", "interrupt"} & set(step)):
        raise ValueError(
            "a scenario step needs a path and value, a record or an interrupt"
        )
    return {**step, "at": seconds}


def run_scenario(
    env: Any,
    steps: Iterable[Mapping[str, Any]],
    until: Optional[float] = None,
    interrupt: Optional[Callable[..., Any]] = None,
):
    """
    Applies each step at its time, in time order and, at equal times, in the order
    given. A step's `at` counts from when this process starts, the start of the run
    when it is started with the model, so it does not depend on `initial_time`.

    A step with a `path` and `value` updates the registry; one with a `record`
    publishes a ground-truth record of the change (`env.publish_label`); one with an
    `interrupt`, `{task, where, cause}`, calls `interrupt(task, where=..., cause=...)`,
    which the builder and YAML pass as `context.interrupt`.

    Args:
        env: The environment.
        steps: The steps, each a mapping as `check_step` takes.
        until: The run's end, a reading of the clock; steps due at or after it are
            dropped, as an event due at exactly `until` does not run.
        interrupt: What an `interrupt` step calls; such steps need it.
    """
    ordered: List[dict] = sorted(
        (check_step(step) for step in steps), key=lambda step: step["at"]
    )
    registry = env.registry
    # `at` counts from the start of the run, the moment this process starts, so a
    # run with an initial_time of 8 h applies a step at 30 min at 8.5 h
    origin = env.now
    for step in ordered:
        at = origin + step["at"]
        if until is not None and at >= until:
            return
        if at > env.now:
            yield _urgent_timeout(env, at - env.now)
        path, record = step.get("path"), step.get("record")
        old = new = None
        if path is not None:
            old = registry.get(path).value
            registry.update(path, step["value"])
            new = registry.get(path).value
        if record is not None:
            env.publish_label(record, path=path, old=old, new=new)
        action = step.get("interrupt")
        if action is not None:
            if interrupt is None:
                raise ValueError("an interrupt step needs an interrupt function")
            interrupt(
                action["task"], where=action.get("where"), cause=action.get("cause")
            )


def _urgent_timeout(env: Any, delay: float) -> Event:
    """A timeout scheduled urgent, so it runs before normal events at its time."""
    event = Event(env)
    event._ok, event._value = True, None
    env.schedule(event, URGENT, delay)
    return event
