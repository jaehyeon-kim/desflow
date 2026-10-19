"""One way to run a model written in any of the three APIs, for replications and
experiments.

A model is a YAML blueprint path, a builder factory (a function with no arguments
that returns a new `SimulationContext`), or a `LowLevelModel`: a function that builds
the model in a given environment with a given sampler, and the model's `sim_id`.
`run_once` runs any of them unpaced, with no ingress and no egress, and returns the
statistics it measured.
"""

import copy
import dataclasses
import pickle
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple, Union

from simpy.events import URGENT, Event

from desflow.core.context import SimulationContext
from desflow.core.environment import DynamicRealtimeEnvironment
from desflow.core.sampler import Sampler
from desflow.utils import time_to_seconds


@dataclass(frozen=True)
class LowLevelModel:
    """
    A model written with the low-level API, for `replicate` and `run_experiment`.

    Attributes:
        function (Callable): Called as `function(env, sampler)` once per run. It
            registers the parameters, builds the resources and starts the processes,
            and draws every random number from `sampler`. It does not set up the
            statistics or the sinks: the runner does. To be drained, it returns the
            processes that start its arrivals, which a drain interrupts at `until`,
            and reports its entities with `env.stats.flow_started` and
            `flow_finished`, so the drain knows when the model is empty.
        sim_id (str): The sim id the parameters are registered under, which the
            statistics' names start with.
    """

    function: Callable[[DynamicRealtimeEnvironment, Sampler], Any]
    sim_id: str


Model = Union[str, Path, Callable[[], SimulationContext], LowLevelModel]


@dataclass
class RunResult:
    """
    What one unpaced run of a model measured.

    Attributes:
        readings (Dict[str, Any]): Every statistic, by its published name.
        streams (List[str]): The names of the random streams the run drew from.
        sim_id (str): The model's sim id.
        parameters (Dict[str, Any]): Every distribution and capacity by registry
            path, as the run started, after its overrides.
    """

    readings: Dict[str, Any]
    streams: List[str]
    sim_id: str
    parameters: Dict[str, Any] = field(default_factory=dict)


def seconds(value: Union[float, str, None]) -> Optional[float]:
    """Reads seconds, or a duration such as `"8 h"`; None stays None.

    Args:
        value: A number of seconds, a duration, or None.

    Returns:
        Optional[float]: The seconds, or None.
    """
    if value is None:
        return None
    return time_to_seconds(value) if isinstance(value, str) else float(value)


def describe(model: Model) -> str:
    """Returns a short name for the model, for messages and experiment evidence.

    Args:
        model: A blueprint path, a builder factory or a `LowLevelModel`.

    Returns:
        str: The path, or the function's module and name.
    """
    if isinstance(model, (str, Path)):
        return str(model)
    target: Any = model.function if isinstance(model, LowLevelModel) else model
    module = getattr(target, "__module__", "")
    name = getattr(target, "__qualname__", repr(target))
    return f"{module}:{name}" if module else name


def check_importable(model: Model) -> None:
    """Checks that another process can run the model, as `workers` above 1 needs.

    Args:
        model: A blueprint path, a builder factory or a `LowLevelModel`.

    Raises:
        ValueError: If the model cannot be sent to another process, such as a
            lambda, a function defined inside another function, or one defined in
            an interactive session or `python -c`.
    """
    target = model.function if isinstance(model, LowLevelModel) else model
    main = sys.modules.get("__main__")
    if (
        getattr(target, "__module__", None) == "__main__"
        and getattr(main, "__file__", None) is None
    ):
        # A function typed into python -c, a REPL or a notebook pickles by name,
        # but a worker process has no such module to find it in
        raise ValueError(
            f"workers above 1 run each replication in another process, which must "
            f"import the model; {describe(model)} was defined in an interactive "
            f"session or python -c, so it cannot be imported there. Put the "
            f"function in a file and import it from there, or use workers=1"
        )
    try:
        pickle.loads(pickle.dumps(model))
    except Exception as exc:
        raise ValueError(
            f"workers above 1 run each replication in another process, which must "
            f"import the model; {describe(model)} cannot be imported there ({exc}). "
            f"Pass a blueprint path or a function defined at the top of a module, "
            f"or use workers=1"
        ) from None


def context_of(model: Model) -> Optional[SimulationContext]:
    """Returns a new context for a blueprint path or a builder factory.

    Args:
        model: A blueprint path, a builder factory or a `LowLevelModel`.

    Returns:
        Optional[SimulationContext]: A context built for this run alone, or None for
            a low-level model.

    Raises:
        TypeError: If a factory returns something other than a `SimulationContext`.
    """
    if isinstance(model, LowLevelModel):
        return None
    if isinstance(model, (str, Path)):
        return SimulationContext.from_yaml(model)
    context = model()
    if not isinstance(context, SimulationContext):
        raise TypeError(
            f"{describe(model)} returned {type(context).__name__}; a builder model "
            f"is a function returning a new SimulationContext. Wrap a low-level "
            f"function in LowLevelModel(function, sim_id)"
        )
    return context


def default_until(model: Model) -> Optional[float]:
    """Returns the blueprint's `run.until` for a blueprint path.

    Args:
        model: A blueprint path, a builder factory or a `LowLevelModel`.

    Returns:
        Optional[float]: The until in seconds, or None for another model or a
            blueprint without one.
    """
    if isinstance(model, (str, Path)):
        from desflow.blueprint.build import build

        return build(model)[1].until
    return None


def _build(
    model: Model,
    seed: int,
    warmup: Optional[float],
    stats: str,
    overrides: Optional[Mapping[str, Any]],
    until: Optional[float] = None,
    stop: Any = None,
    drain: Any = None,
) -> Tuple[
    DynamicRealtimeEnvironment,
    Sampler,
    str,
    Callable[[], None],
    Optional[SimulationContext],
    Any,
]:
    """Builds one run of a model up to the moment its clock starts.

    Also returns what restores the model's parameters after the run, and the
    context of a blueprint or builder model, which `start()` has started: its
    `finish()` runs it to the end `run()` would, `run.stop` and `run.drain`
    included.
    """
    context = context_of(model)
    if context is not None:
        context.factor = 0.0
        context.go_live_at = None
        if warmup is not None:
            context.warmup = warmup
        context.start(
            until,
            stop,
            drain,
            seed=seed,
            overrides=overrides,
            stats=stats,
            egress=False,
            ingress=False,
        )
        assert context.sampler is not None
        # A new context per run, so nothing is shared with the next run
        return (
            context.env,
            context.sampler,
            context.sim_id,
            lambda: None,
            context,
            None,
        )
    assert isinstance(model, LowLevelModel)
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.stats.configure(model.sim_id, stats)
    sampler = Sampler(seed=seed)
    arrivals = model.function(env, sampler)
    restore = _keep(env)
    try:
        for path, value in (overrides or {}).items():
            env.registry.set_exactly(path, value)
    except ValueError:
        restore()
        raise
    if warmup is not None:
        env.process(_end_warmup(env, warmup))
    return env, sampler, model.sim_id, restore, None, arrivals


def _keep(env: DynamicRealtimeEnvironment) -> Callable[[], None]:
    """Returns what puts every registered config back as it is now.

    The registry writes each change through to the config objects it was given, and
    a low-level model usually registers objects that live in its module. Without
    this, a variant's value or a live change in one run would be where the next run
    starts.
    """
    saved = [
        (
            config,
            copy.deepcopy(vars(config) if not isinstance(config, dict) else config),
        )
        for config in env.registry.configs().values()
    ]

    def restore() -> None:
        for config, values in saved:
            if isinstance(config, dict):
                config.clear()
                config.update(values)
            else:
                vars(config).update(values)

    return restore


def inspect_model(model: Model, overrides: Mapping[str, Any]) -> Dict[str, Any]:
    """Builds the model with `overrides` without running it, and returns its parameters.

    An experiment calls it for each design before running anything, so a bad path fails
    at once rather than after other designs have run.

    Args:
        model: A blueprint path, a builder factory or a `LowLevelModel`.
        overrides: Registry paths and the values they take.

    Returns:
        Dict[str, Any]: Every distribution and capacity by registry path.

    Raises:
        ValueError: If a path does not exist or its value is refused.
    """
    env, _, _, restore, context, _ = _build(model, 0, None, "none", overrides)
    parameters = _parameters(env)
    if context is not None:
        context.close()
    else:
        env.teardown()
    restore()
    return parameters


def run_once(
    model: Model,
    *,
    seed: int,
    until: float,
    warmup: Optional[float] = None,
    stats: str = "basic",
    overrides: Optional[Mapping[str, Any]] = None,
    stop: Any = None,
    drain: Any = None,
) -> RunResult:
    """
    Runs a model once, unpaced, with no ingress and no egress.

    Args:
        model: A YAML path, a builder factory or a `LowLevelModel`.
        seed: The run's seed.
        until: The simulation time the arrivals stop at, and the run too unless a
            `stop` ends it earlier or a `drain` runs on.
        warmup: The end of the warm-up in simulation seconds; None keeps the
            model's own.
        stats: The lowest statistics level the run keeps.
        overrides: Registry paths and the values they take before the clock starts.
        stop: As for `SimulationContext.run`: `{"signal": name}`,
            `{"condition": "..."}`, or for a blueprint or builder model a signal's
            name; for a `LowLevelModel`, a function of the environment returning
            the event that ends the run. None keeps a blueprint's `run.stop`.
        drain: As for `SimulationContext.run`: True, or `{"limit": duration}`.
            None keeps a blueprint's `run.drain`.

    Returns:
        RunResult: The statistics at the end of the run, the random streams used and
            the parameters it started with.

    Raises:
        ValueError: If an override's path does not exist or its value is refused,
            or a low-level model is drained without returning its arrivals.
    """
    if isinstance(stop, str) and not isinstance(model, LowLevelModel):
        stop = {"signal": stop}
    env, sampler, sim_id, restore, context, arrivals = _build(
        model, seed, warmup, stats, overrides, until, stop, drain
    )
    parameters = _parameters(env)
    try:
        if context is not None:
            # The model's own end, or the one given: until, a stop or a drain
            context.finish()
        else:
            _run_low_level(env, until, stop, drain, arrivals)
    finally:
        if context is None:
            env.teardown()
        restore()
    return RunResult(
        readings=env.stats.readings(),
        streams=sampler.streams,
        sim_id=sim_id,
        parameters=parameters,
    )


def _run_low_level(
    env: DynamicRealtimeEnvironment,
    until: float,
    stop: Any,
    drain: Any,
    arrivals: Any,
) -> None:
    """Runs a low-level model to `until`, a `stop` event, or the end of a drain."""
    if stop is None:
        env.run(until=until)
    else:
        # Urgent, as SimPy schedules its own `until`, so no event due at `until`
        # runs before the run ends there
        horizon = Event(env)
        horizon._ok, horizon._value = True, None
        env.schedule(horizon, URGENT, until - env.now)
        env.run(until=stop(env) | horizon)
    if not drain or (stop is not None and env.now < until):
        return
    processes = list(arrivals or [])
    if not processes:
        raise ValueError(
            "a low-level model drains only when its function returns the processes "
            "that start its arrivals"
        )
    for process in processes:
        if process.is_alive:
            process.interrupt("arrivals stopped")
            process.defused = True
    limit = seconds(drain.get("limit")) if isinstance(drain, Mapping) else None
    last = float("inf") if limit is None else until + limit
    while env.stats.in_system() > 0 and env.peek() < last:
        env.step()
    if env.stats.in_system() > 0 and last < float("inf") and env.now < last:
        env.run(until=last)


def _parameters(env: DynamicRealtimeEnvironment) -> Dict[str, Any]:
    """Every distribution and capacity by path, as plain values."""
    found: Dict[str, Any] = {}
    for path, config in env.registry.configs().items():
        if dataclasses.is_dataclass(config) and not isinstance(config, type):
            values = dataclasses.asdict(config)
            values.pop("stream", None)
            found[path] = {k: v for k, v in values.items() if v is not None}
        elif config:
            found[path] = dict(config)
    return found


def _end_warmup(env: DynamicRealtimeEnvironment, warmup: float):
    """Resets the statistics at the end of the warm-up."""
    yield env.timeout(warmup)
    env.stats.reset()
