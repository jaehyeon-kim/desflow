"""Scenarios: registry changes and ground-truth records at set simulation times.

`run_scenario` is a SimPy process any API can start. Each step applies at its time
on the simulation clock, so a scenario repeats exactly and works at `factor: 0`. A
step is scheduled as an urgent event, as SimPy schedules its own `until`, so it
applies before every other model event at that time: an arrival gap drawn at that
moment already uses a new rate. A step at or after the run's `until` never applies.
"""

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
)

from simpy.events import Event

from desflow.core.environment import schedule_urgent
from desflow.core.expressions import Expression
from desflow.core.registry import ParamRejected
from desflow.utils import checked_seconds, suggestion
from desflow.values.models import has_generator

if TYPE_CHECKING:
    from desflow.core.registry import SimulationRegistry

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
            value, a value has no path, the step does nothing, an interrupt names
            no task (a `handle` is the interrupt key of a task, not of a step), or
            an interrupt's `where` is not a valid expression.
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
    try:
        seconds = checked_seconds(at, "a scenario step's at", ends=True)
    except ValueError as exc:
        raise ValueError(f"{exc}; at must be 0 or more") from None
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
    where = action.get("where") if action is not None else None
    if isinstance(where, str):
        try:
            Expression(where)
        except ValueError as exc:
            raise ValueError(f"a scenario step's interrupt where: {exc}") from None
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


def walk_steps(
    steps: Iterable[Tuple[float, Optional[str], Any]],
    registry: "SimulationRegistry",
    is_member: Callable[[str], bool],
) -> Iterator[Tuple[int, Optional[str], Any]]:
    """Applies scenario steps to a scratch registry in the order the run applies
    them, and yields each step's index with the problem found, so each caller words
    its own message.

    The steps go in time order, and `sorted` is stable, so steps at one time keep
    the order they are written in. A step with no path or with the path of a family
    member is not applied, and nor is a generated value, which is known only when
    the step is due.

    Args:
        steps: Each step's time, path and value, in the order they are written.
        registry: The scratch registry, which the walk changes.
        is_member: Whether a path names an attribute of a member of a family.

    Yields:
        Tuple[int, Optional[str], Any]: `(index, None, None)` for a step with no
        problem; `(index, "path", None)` when its path is not in the registry;
        `(index, "value", reason)` when the registry refuses the value.
        The walk ends after the first problem.
    """
    ordered = sorted(enumerate(steps), key=lambda item: item[1][0])
    for index, (_at, path, value) in ordered:
        if path is None or is_member(path):
            yield index, None, None
            continue
        try:
            registry.get(path)
        except KeyError:
            yield index, "path", None
            return
        if has_generator(value):
            yield index, None, None
            continue
        try:
            registry.change(path, value)
        except ParamRejected as exc:
            yield index, "value", exc.reason
            return
        yield index, None, None


def _urgent_timeout(env: Any, delay: float) -> Event:
    """A timeout scheduled urgent, so it runs before normal events at its time."""
    return schedule_urgent(env, Event(env), delay)
