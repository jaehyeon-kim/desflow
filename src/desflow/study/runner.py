"""Replications: one model run with many seeds, and its statistics summarised.

`replicate` runs a model N times with seeds spawned from one seed, each unpaced with
no ingress and no egress, and reads the built-in statistics at the end of each run.
It returns every value with its seed, and the mean and Student t half-width of each
measure. Studies use `run_replications` to run each variant the same way.
"""

import logging
import re
import secrets
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Union

import numpy as np

from desflow.config import STATS_LEVELS
from desflow.study.intervals import interval
from desflow.study.models import (
    LowLevelModel,
    Model,
    RunResult,
    check_importable,
    context_of,
    default_until,
    run_once,
    seconds,
)

logger = logging.getLogger(__name__)

# A statistic's level, read from its name: means and maxima of waits and services
# need `typical`, percentiles need `all`; every other statistic is at `basic` or
# below. Names that no level publishes are refused after the first replication.
_TYPICAL_NAME = re.compile(r"\.(wait|service)_(mean|max)$")
_ALL_NAME = re.compile(r"\.(wait|service)_p\d+$")
# Counts published once they are above 0, so a run without one has none to read.
_COUNTED_ONCE_SEEN = ("finished", "balked", "reneged")


def level_for(measures: Optional[Iterable[str]]) -> str:
    """
    Returns the lowest statistics level that publishes every measure.

    Args:
        measures: Statistic names such as `tasks.pack.wait_mean`; None means every
            statistic up to `typical`.

    Returns:
        str: `basic`, `typical` or `all`.
    """
    if measures is None:
        return "typical"
    level = "basic"
    for name in measures:
        needed = (
            "all"
            if _ALL_NAME.search(name)
            else "typical"
            if _TYPICAL_NAME.search(name)
            else "basic"
        )
        if STATS_LEVELS.index(needed) > STATS_LEVELS.index(level):
            level = needed
    return level


def spawn_seeds(seed: int, n: int) -> List[int]:
    """
    Returns `n` plain integer seeds spawned from `seed`.

    Each is a child of `numpy.random.SeedSequence(seed)`, turned into one integer
    that `desflow run --seed` or `random_seed` takes, so any replication repeats on
    its own.

    Args:
        seed: The seed of the whole set.
        n: How many seeds.

    Returns:
        List[int]: The seeds, the same list for the same `seed` every time.
    """
    return [
        int(child.generate_state(1)[0])
        for child in np.random.SeedSequence(seed).spawn(n)
    ]


@dataclass
class Replications:
    """
    The result of `replicate`: every value with its seed, and a summary per measure.

    Attributes:
        sim_id (str): The model's sim id.
        seeds (List[int]): The seed of each replication, in order. Replication `i`
            (from 1) ran with `seeds[i - 1]`.
        measures (List[str]): The statistics summarised, such as
            `tasks.pack.wait_mean`.
        values (Dict[str, List[float]]): Each measure's value in each replication.
        level (float): The confidence level of the half-widths.
        streams (List[List[str]]): The random streams each replication drew from.
    """

    sim_id: str
    seeds: List[int]
    measures: List[str]
    values: Dict[str, List[float]]
    level: float = 0.95
    streams: List[List[str]] = field(default_factory=list)

    @property
    def table(self) -> List[Dict[str, Any]]:
        """One row per replication and measure: `replication` (from 1), `seed`,
        `measure` and `value`."""
        return [
            {"replication": i, "seed": seed, "measure": measure, "value": value}
            for measure in self.measures
            for i, (seed, value) in enumerate(
                zip(self.seeds, self.values[measure]), start=1
            )
        ]

    @property
    def summary(self) -> List[Dict[str, Any]]:
        """One row per measure: `measure`, `n`, `mean`, `half_width` and `level`."""
        rows = []
        for measure in self.measures:
            result = interval(self.values[measure], self.level)
            rows.append(
                {
                    "measure": measure,
                    "n": result.n,
                    "mean": result.mean,
                    "half_width": result.half_width,
                    "level": self.level,
                }
            )
        return rows

    def summary_text(self) -> str:
        """The summary as a table: each measure's n, mean and half-width."""
        rows = self.summary
        width = max(len("measure"), *(len(row["measure"]) for row in rows))
        heading = f"half-width ({self.level:.0%})"
        lines = [f"{'measure':<{width}}  {'n':>4}  {'mean':>12}  {heading:>18}"]
        for row in rows:
            lines.append(
                f"{row['measure']:<{width}}  {row['n']:>4}  {row['mean']:>12.4g}  "
                f"{row['half_width']:>18.4g}"
            )
        return "\n".join(lines)

    def telemetry(self) -> List[tuple]:
        """
        The telemetry records the sinks receive, as `(path_id, value)` pairs.

        One per measure and replication, `<sim_id>.stats.<measure>.replication.<i>`,
        then the mean, half-width and count across replications,
        `<sim_id>.stats.<measure>.mean`, `.half_width` and `.n`.
        """
        records: List[tuple] = []
        prefix = f"{self.sim_id}.stats"
        for row in self.table:
            name = f"{prefix}.{row['measure']}.replication.{row['replication']}"
            records.append((name, row["value"]))
        for row in self.summary:
            for key in ("mean", "half_width", "n"):
                records.append((f"{prefix}.{row['measure']}.{key}", row[key]))
        return records


def replicate(
    model: Model,
    n: int,
    *,
    seed: Optional[int],
    until: Union[float, str, None],
    warmup: Union[float, str, None] = None,
    measures: Optional[Sequence[str]] = None,
    level: float = 0.95,
    workers: int = 1,
    overrides: Optional[Mapping[str, Any]] = None,
) -> Replications:
    """
    Runs a model `n` times with spawned seeds and summarises its statistics.

    Each replication runs unpaced, with no ingress and no egress, and reads the
    built-in statistics at its end. When the model has sinks (a blueprint's
    `egress`, or a builder's `add_egress`), they receive the summary as telemetry
    once, at the end: each replication's value and the mean, half-width and count of
    each measure.

    Args:
        model: A YAML blueprint path, a builder factory (a function with no
            arguments returning a new `SimulationContext`), or a `LowLevelModel`.
        n: How many replications, 2 or more.
        seed: The seed the replications' seeds are spawned from. None draws one and
            logs it.
        until: The simulation time each replication stops at, in seconds or as a
            duration such as `"1 h"`. None uses a blueprint's `run.until`.
        warmup: The end of each replication's warm-up; None keeps the model's own.
        measures: Statistic names without the sim id, such as
            `tasks.pack.wait_mean`. None summarises every statistic up to `typical`.
        level: The confidence level of the half-widths. Defaults to 0.95.
        workers: Replications run at once, each in its own process. Above 1 the
            model must be a path or a function defined at the top of a module.
        overrides: Registry paths and the values they take in every replication.

    Returns:
        Replications: Every value with its seed, and the summary.

    Raises:
        ValueError: If `n` is below 2, `until` is missing, the level is not between
            0 and 1, a measure is not a statistic of the model, or the model cannot
            be sent to another process when `workers` is above 1.
    """
    if n < 2:
        raise ValueError(f"replications need n of 2 or more, got {n}")
    if not 0.0 < level < 1.0:
        raise ValueError(f"level must be between 0 and 1, got {level}")
    if seed is None:
        seed = secrets.randbelow(2**32)
        logger.info(f"No seed was given, so the replications use {seed}.")
    seeds = spawn_seeds(seed, n)
    results = run_replications(
        model,
        seeds,
        until=until,
        warmup=warmup,
        stats=level_for(measures),
        workers=workers,
        overrides=overrides,
    )
    names = select_measures(results, measures)
    replications = Replications(
        sim_id=results[0].sim_id,
        seeds=seeds,
        measures=names,
        values={m: values_of(results, m) for m in names},
        level=level,
        streams=[r.streams for r in results],
    )
    _publish(model, replications, until)
    return replications


def run_replications(
    model: Model,
    seeds: Sequence[int],
    *,
    until: Union[float, str, None],
    warmup: Union[float, str, None] = None,
    stats: str = "basic",
    workers: int = 1,
    overrides: Optional[Mapping[str, Any]] = None,
) -> List[RunResult]:
    """
    Runs a model once per seed and returns what each run measured, in seed order.

    Args:
        model: A YAML path, a builder factory or a `LowLevelModel`.
        seeds: One seed per run.
        until: The simulation time each run stops at; None uses a blueprint's
            `run.until`.
        warmup: The end of each run's warm-up; None keeps the model's own.
        stats: The lowest statistics level each run keeps.
        workers: Runs at once, each in its own process.
        overrides: Registry paths and the values they take in every run.

    Returns:
        List[RunResult]: One per seed.

    Raises:
        ValueError: If `until` is missing, or the model cannot be sent to another
            process when `workers` is above 1.
    """
    stop = seconds(until)
    if stop is None:
        stop = default_until(model)
    if stop is None:
        raise ValueError(
            "replications need an end time: pass until, or set run.until in the "
            "blueprint"
        )
    options = dict(
        until=stop, warmup=seconds(warmup), stats=stats, overrides=dict(overrides or {})
    )
    if workers > 1 and len(seeds) > 1:
        check_importable(model)
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = [
                pool.submit(run_once, model, seed=s, **options)  # type: ignore[arg-type]
                for s in seeds
            ]
            return [future.result() for future in futures]
    return [run_once(model, seed=s, **options) for s in seeds]  # type: ignore[arg-type]


def select_measures(
    results: Sequence[RunResult], measures: Optional[Sequence[str]]
) -> List[str]:
    """
    Returns the measures to summarise, refusing one no run published.

    Args:
        results: The runs.
        measures: The requested names, or None for every statistic the runs
            published, in the order of the first run.

    Returns:
        List[str]: The names without the sim id.

    Raises:
        ValueError: If a requested measure is not a statistic of any run.
    """
    prefix = f"{results[0].sim_id}.stats."
    published: Dict[str, None] = {}
    for result in results:
        for name, value in result.readings.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                published.setdefault(name.removeprefix(prefix), None)
    if measures is None:
        return list(published)
    unknown = [m for m in measures if m not in published]
    if unknown:
        raise ValueError(
            f"no statistic called {', '.join(unknown)}; this model publishes "
            f"{', '.join(published)}"
        )
    return list(dict.fromkeys(measures))


def values_of(results: Sequence[RunResult], measure: str) -> List[float]:
    """
    Returns one measure's value in each run.

    A count that is published once it is above 0, such as `reneged`, is 0 in a run
    that did not publish it.

    Raises:
        ValueError: If another statistic is missing from a run, such as a mean wait
            in a run where no task started.
    """
    values = []
    for i, result in enumerate(results, start=1):
        value = result.readings.get(f"{result.sim_id}.stats.{measure}")
        if value is None and measure.rsplit(".", 1)[-1] in _COUNTED_ONCE_SEEN:
            value = 0
        if value is None:
            raise ValueError(
                f"{measure} has no value in run {i}: nothing it measures happened "
                f"in that run. Run longer, or choose another measure"
            )
        values.append(value)
    return values


def _publish(model: Model, replications: Replications, until: Any) -> None:
    """Sends the summary to the model's sinks, once, stamped at the end time.

    The model is built again with its sinks but never run, so a model without
    sinks publishes nothing.
    """
    if isinstance(model, LowLevelModel):
        return
    context = context_of(model)
    assert context is not None
    stop = seconds(until)
    if stop is None:
        stop = default_until(model)
    context.factor = 0.0
    context.go_live_at = None
    context.warmup = None
    context.initial_time = float(stop or 0.0)
    context._prepare(seed=replications.seeds[0], egress=True)
    # Only the summary is sent: the model's own statistics are not published
    context.env.stats.configure(replications.sim_id, "none")
    try:
        for path_id, value in replications.telemetry():
            context.env.publish_telemetry(path_id, value)
    finally:
        context.env.teardown()
