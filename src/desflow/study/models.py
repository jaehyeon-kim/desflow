"""One way to run a model written in any of the three APIs, for replications and studies.

A model is a YAML blueprint path, a builder factory (a function with no arguments
that returns a new `SimulationContext`), or a `LowLevelModel`: a function that builds
the model in a given environment with a given sampler, and the model's `sim_id`.
`run_once` runs any of them unpaced, with no ingress and no egress, and returns the
statistics it measured.
"""

import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Union

from desflow.core.context import SimulationContext
from desflow.core.environment import DynamicRealtimeEnvironment
from desflow.core.sampler import Sampler
from desflow.utils import time_to_seconds


@dataclass(frozen=True)
class LowLevelModel:
    """
    A model written with the low-level API, for `replicate` and `run_study`.

    Attributes:
        function (Callable): Called as `function(env, sampler)` once per run. It
            registers the parameters, builds the resources and starts the processes,
            and draws every random number from `sampler`. It does not set up the
            statistics or the sinks: the runner does.
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
    """

    readings: Dict[str, Any]
    streams: List[str]
    sim_id: str


def seconds(value: Union[float, str, None]) -> Optional[float]:
    """Reads seconds, or a duration such as `"8 h"`; None stays None."""
    if value is None:
        return None
    return time_to_seconds(value) if isinstance(value, str) else float(value)


def describe(model: Model) -> str:
    """A short name for the model, for messages and study evidence."""
    if isinstance(model, (str, Path)):
        return str(model)
    target: Any = model.function if isinstance(model, LowLevelModel) else model
    module = getattr(target, "__module__", "")
    name = getattr(target, "__qualname__", repr(target))
    return f"{module}:{name}" if module else name


def check_importable(model: Model) -> None:
    """
    Checks that another process can run the model, as `workers` above 1 needs.

    Raises:
        ValueError: If the model cannot be sent to another process, such as a
            lambda or a function defined inside another function.
    """
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
    """A new context for a YAML path or a builder factory; None for a low-level model.

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
    """The blueprint's `run.until` for a YAML path, else None."""
    if isinstance(model, (str, Path)):
        from desflow.blueprint.build import build

        return build(model)[1].until
    return None


def run_once(
    model: Model,
    *,
    seed: int,
    until: float,
    warmup: Optional[float] = None,
    stats: str = "basic",
    overrides: Optional[Mapping[str, Any]] = None,
) -> RunResult:
    """
    Runs a model once, unpaced, with no ingress and no egress.

    Args:
        model: A YAML path, a builder factory or a `LowLevelModel`.
        seed: The run's seed.
        until: The simulation time the run stops at.
        warmup: The end of the warm-up in simulation seconds; None keeps the
            model's own.
        stats: The lowest statistics level the run keeps.
        overrides: Registry paths and the values they take before the clock starts.

    Returns:
        RunResult: The statistics at the end of the run, and the random streams used.

    Raises:
        ValueError: If an override's path does not exist or its value is refused.
    """
    context = context_of(model)
    if context is not None:
        context.factor = 0.0
        context.go_live_at = None
        if warmup is not None:
            context.warmup = warmup
        context._prepare(seed=seed, overrides=overrides, stats=stats, egress=False)
        env = context.env
        sampler = context.sampler
        sim_id = context.sim_id
    else:
        assert isinstance(model, LowLevelModel)
        env = DynamicRealtimeEnvironment(factor=0.0)
        env.stats.configure(model.sim_id, stats)
        sampler = Sampler(seed=seed)
        model.function(env, sampler)
        for path, value in (overrides or {}).items():
            env.registry.set_exactly(path, value)
        if warmup is not None:
            env.process(_end_warmup(env, warmup))
        sim_id = model.sim_id
    try:
        env.run(until=until)
    finally:
        env.teardown()
    assert sampler is not None
    return RunResult(
        readings=env.stats.readings(), streams=sampler.streams, sim_id=sim_id
    )


def _end_warmup(env: DynamicRealtimeEnvironment, warmup: float):
    """Resets the statistics at the end of the warm-up."""
    yield env.timeout(warmup)
    env.stats.reset()
