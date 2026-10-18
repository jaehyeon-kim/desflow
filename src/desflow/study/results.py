"""A study's designs, results, comparisons, checks and artifacts.

A design is one variant at one point of the sweep. Each design runs the study's
replications; this module turns those runs into rows (one per run and statistic,
one per design and statistic, one per comparison and one per check), reads the
observed values a study compares with, and writes and reads the artifact files a
report is built from.
"""

import csv
import itertools
import json
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Sequence

import numpy as np

from desflow.study.intervals import interval
from desflow.study.models import RunResult
from desflow.study.runner import spawn_seeds, values_of
from desflow.study.spec import StudySpec
from desflow.utils import time_to_seconds

STUDY_FILE = "study.json"
RESULTS_FILE = "results.csv"
SUMMARY_FILE = "summary.csv"
COMPARISONS_FILE = "comparisons.csv"
CHECKS_FILE = "checks.csv"
REPORT_FILE = "report.md"

# The unit and the population of each statistic, read from its name. A wait is
# measured on the instances that started work, a service on those that finished; a
# time average over the horizon; a count over the horizon; any other reading at the
# end of the run.
_DURATION = re.compile(r"\.(wait|service)_(mean|max|p\d+)$")
_AVERAGE = re.compile(r"_avg$")
_PERCENT = re.compile(r"\.utilization(_avg)?$")
_AMOUNT = re.compile(r"^containers\..*\.(capacity|level|level_avg)$")
_UNITS = {
    "s": "s",
    "sec": "s",
    "second": "s",
    "seconds": "s",
    "%": "percent",
    "percent": "percent",
    "count": "count",
    "counts": "count",
    "amount": "amount",
}


def unit_of(measure: str) -> str:
    """The unit of a statistic: `s`, `percent`, `amount` or `count`."""
    if _DURATION.search(measure):
        return "s"
    if _PERCENT.search(measure):
        return "percent"
    if _AMOUNT.search(measure):
        return "amount"
    return "count"


def population_of(measure: str) -> str:
    """What a statistic is measured on: `started instances` for a wait, which is
    recorded when an instance starts work; `finished instances` for a service,
    recorded when it finishes; `time average`; `count over the horizon`; or
    `end of run`."""
    if _DURATION.search(measure):
        return "started instances" if ".wait_" in measure else "finished instances"
    if _AVERAGE.search(measure):
        return "time average"
    if measure.split(".")[0] in ("tasks", "flows"):
        return "count over the horizon"
    return "end of run"


@dataclass
class Design:
    """
    One variant at one point of the sweep, with the seeds its replications use.

    Attributes:
        name (str): The variant's name, followed by the swept values, such as
            `three Shop.arrival.orders.rate=0.5`.
        variant (str): The variant's name.
        point (Dict[str, Any]): The swept path and value, by path; empty without a
            sweep.
        overrides (Dict[str, Any]): Every registry path the design sets.
        seeds (List[int]): The seed of each replication.
    """

    name: str
    variant: str
    point: Dict[str, Any]
    overrides: Dict[str, Any]
    seeds: List[int]
    runs: List[RunResult] = field(default_factory=list, repr=False)


def designs(spec: StudySpec) -> List[Design]:
    """
    Every design of a study: each variant at each point of the sweep.

    With common pairing every design uses the same seeds, spawned from the study's
    seed; with independent pairing each design's seeds are spawned from a seed of
    its own.
    """
    paths = list(spec.sweep)
    points = [
        dict(zip(paths, values))
        for values in itertools.product(*(spec.sweep[p] for p in paths))
    ]
    found = []
    for variant, settings in spec.variants.items():
        for point in points:
            name = " ".join(
                [variant, *(f"{path}={value}" for path, value in point.items())]
            )
            found.append(Design(name, variant, point, {**settings, **point}, []))
    if spec.pairing == "common":
        seeds = spawn_seeds(spec.seed, spec.replications)
        for design in found:
            design.seeds = list(seeds)
    else:
        children = np.random.SeedSequence(spec.seed).spawn(len(found))
        for design, child in zip(found, children):
            design.seeds = [
                int(grandchild.generate_state(1)[0])
                for grandchild in child.spawn(spec.replications)
            ]
    return found


def count_names(runs: Sequence[RunResult]) -> List[str]:
    """Every count the runs published: whole-number statistics of tasks and flows."""
    names: Dict[str, None] = {}
    for run in runs:
        prefix = f"{run.sim_id}.stats."
        for name, value in run.readings.items():
            short = name.removeprefix(prefix)
            if short.split(".")[0] in ("tasks", "flows") and type(value) is int:
                names.setdefault(short, None)
    return list(names)


def _count(run: RunResult, name: str) -> int:
    """A count in one run; one published only once above 0 is 0 when absent."""
    return int(run.readings.get(f"{run.sim_id}.stats.{name}", 0))


def result_rows(
    all_designs: Sequence[Design], measures: Sequence[str], counts: Sequence[str]
) -> List[Dict[str, Any]]:
    """One row per design, replication and statistic: the measures, then the counts."""
    rows = []
    for design in all_designs:
        values = {m: values_of(design.runs, m) for m in measures}
        values.update({c: [_count(r, c) for r in design.runs] for c in counts})
        for name, series in values.items():
            for i, (seed, value) in enumerate(zip(design.seeds, series), start=1):
                rows.append(
                    {
                        "design": design.name,
                        "variant": design.variant,
                        **design.point,
                        "replication": i,
                        "seed": seed,
                        "statistic": name,
                        "kind": "measure" if name in measures else "count",
                        "unit": unit_of(name),
                        "population": population_of(name),
                        "value": value,
                    }
                )
    return rows


def _series(rows: Sequence[Dict[str, Any]], design: str, name: str) -> List[float]:
    return [
        float(r["value"])
        for r in rows
        if r["design"] == design and r["statistic"] == name
    ]


def summary_rows(
    all_designs: Sequence[Design],
    rows: Sequence[Dict[str, Any]],
    names: Sequence[str],
    level: float,
) -> List[Dict[str, Any]]:
    """One row per design and statistic: n, mean, half-width and bounds."""
    found = []
    for design in all_designs:
        for name in names:
            result = interval(_series(rows, design.name, name), level)
            found.append(
                {
                    "design": design.name,
                    "variant": design.variant,
                    **design.point,
                    "statistic": name,
                    "unit": unit_of(name),
                    "population": population_of(name),
                    "n": result.n,
                    "mean": result.mean,
                    "half_width": result.half_width,
                    "low": result.low,
                    "high": result.high,
                    "level": level,
                }
            )
    return found


def comparison_rows(
    spec: StudySpec,
    all_designs: Sequence[Design],
    rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    One row per compared pair, sweep point and measure.

    The difference is the variant's value minus the baseline's, seed by seed, and
    its interval is a Student t interval on those differences. The random streams
    each side drew from are listed, so a reader sees whether the two used the same
    ones.
    """
    found = []
    by_key = {(d.variant, tuple(d.point.items())): d for d in all_designs}
    for entry in spec.compare:
        for (variant, point), design in by_key.items():
            if variant != entry.variant:
                continue
            baseline = by_key[(entry.baseline, point)]
            ours = set(itertools.chain.from_iterable(r.streams for r in design.runs))
            theirs = set(
                itertools.chain.from_iterable(r.streams for r in baseline.runs)
            )
            for measure in spec.measures:
                before = _series(rows, baseline.name, measure)
                after = _series(rows, design.name, measure)
                result = interval([a - b for a, b in zip(after, before)], spec.level)
                found.append(
                    {
                        "baseline": baseline.name,
                        "variant": design.name,
                        **dict(point),
                        "measure": measure,
                        "n": result.n,
                        "baseline_mean": sum(before) / len(before),
                        "variant_mean": sum(after) / len(after),
                        "difference": result.mean,
                        "half_width": result.half_width,
                        "low": result.low,
                        "high": result.high,
                        "level": spec.level,
                        "streams_both": ";".join(sorted(ours & theirs)),
                        "streams_baseline_only": ";".join(sorted(theirs - ours)),
                        "streams_variant_only": ";".join(sorted(ours - theirs)),
                    }
                )
    return found


def check_rows(
    spec: StudySpec, summary: Sequence[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    One row per check, design and bound.

    `on: interval` compares the far end of the interval with the bound: the upper
    end with `max`, the lower end with `min`. `on: mean` compares the mean.
    """
    found = []
    for check in spec.checks:
        for row in summary:
            if row["statistic"] != check.measure:
                continue
            if check.variant is not None and row["variant"] != check.variant:
                continue
            for bound, limit in (("max", check.max), ("min", check.min)):
                if limit is None:
                    continue
                if check.on == "mean":
                    value = row["mean"]
                else:
                    value = row["high"] if bound == "max" else row["low"]
                passed = value <= limit if bound == "max" else value >= limit
                found.append(
                    {
                        "design": row["design"],
                        "variant": row["variant"],
                        **{k: row[k] for k in spec.sweep},
                        "measure": check.measure,
                        "on": check.on,
                        "bound": bound,
                        "limit": limit,
                        "value": value,
                        "passed": passed,
                    }
                )
    return found


def read_observed(spec: StudySpec, folder: Path) -> List[Dict[str, Any]]:
    """
    Reads the observed CSV and checks each row against the measure it names.

    Raises:
        ValueError: If a column is missing, a measure is not one of the study's,
            or a row's unit, population or horizon differs from the simulated
            measure's.
    """
    if spec.observed is None:
        return []
    path = folder / spec.observed.file
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    needed = ("measure", "value", "unit", "population", "horizon")
    if rows and any(column not in rows[0] for column in needed):
        raise ValueError(f"{path} needs the columns {', '.join(needed)}")
    horizon = spec.until - (spec.warmup or 0.0)
    found = []
    for number, row in enumerate(rows, start=2):
        where = f"{path}:{number}"
        measure = row["measure"].strip()
        if measure not in spec.measures:
            raise ValueError(f"{where}: {measure} is not one of the study's measures")
        unit = _UNITS.get(row["unit"].strip().lower())
        if unit != unit_of(measure):
            raise ValueError(
                f"{where}: {measure} is in {unit_of(measure)}, not {row['unit']!r}; "
                f"convert the observed value"
            )
        population = row["population"].strip()
        if population != population_of(measure):
            raise ValueError(
                f"{where}: {measure} is measured on {population_of(measure)!r}, not "
                f"{population!r}"
            )
        text = row["horizon"].strip()
        seconds = time_to_seconds(text) if re.search(r"[a-z]", text) else float(text)
        if not math.isclose(seconds, horizon):
            raise ValueError(
                f"{where}: the observed horizon is {seconds:g} s, but the study "
                f"measures {horizon:g} s (until minus warmup)"
            )
        found.append(
            {
                "measure": measure,
                "value": float(row["value"]),
                "unit": unit,
                "population": population,
                "horizon": horizon,
            }
        )
    return found


def observed_rows(
    spec: StudySpec, observed: Sequence[Dict[str, Any]], summary: Sequence[Dict]
) -> List[Dict[str, Any]]:
    """Each observed value beside the simulated interval of the observed variant."""
    if spec.observed is None:
        return []
    variant = spec.observed.variant or next(iter(spec.variants))
    found = []
    for item in observed:
        for row in summary:
            if row["variant"] != variant or row["statistic"] != item["measure"]:
                continue
            found.append(
                {
                    **item,
                    "design": row["design"],
                    "mean": row["mean"],
                    "half_width": row["half_width"],
                    "low": row["low"],
                    "high": row["high"],
                    "inside": row["low"] <= item["value"] <= row["high"],
                }
            )
    return found


def write_csv(path: Path, rows: Sequence[Dict[str, Any]], columns: Sequence[str]):
    """Writes rows with these columns, in this order."""
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns))
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c, "") for c in columns})


def read_csv(path: Path) -> List[Dict[str, str]]:
    """Reads a CSV the study wrote; a missing file has no rows."""
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_json(path: Path, data: Dict[str, Any]) -> None:
    """Writes the study's settings and evidence, indented, keys in order."""
    path.write_text(json.dumps(data, indent=2, default=str) + "\n", encoding="utf-8")


def read_json(path: Path) -> Dict[str, Any]:
    """Reads `study.json`."""
    return json.loads(path.read_text(encoding="utf-8"))


def columns(spec_sweep: Sequence[str], kind: str) -> List[str]:
    """The columns of one artifact file, with the swept paths after the variant."""
    sweep = list(spec_sweep)
    return {
        RESULTS_FILE: ["design", "variant", *sweep, "replication", "seed"]
        + ["statistic", "kind", "unit", "population", "value"],
        SUMMARY_FILE: ["design", "variant", *sweep, "statistic", "unit"]
        + ["population", "n", "mean", "half_width", "low", "high", "level"],
        COMPARISONS_FILE: ["baseline", "variant", *sweep, "measure", "n"]
        + ["baseline_mean", "variant_mean", "difference", "half_width", "low"]
        + ["high", "level", "streams_both", "streams_baseline_only"]
        + ["streams_variant_only"],
        CHECKS_FILE: ["design", "variant", *sweep, "measure", "on", "bound"]
        + ["limit", "value", "passed"],
    }[kind]


@dataclass
class StudyResult:
    """
    What a study found, as `run_study` returns it and its artifacts hold it.

    Attributes:
        spec (StudySpec): The study's settings.
        output (Path): The folder the artifacts were written to.
        designs (List[Design]): Each variant at each sweep point, with its seeds.
        results (List[Dict]): One row per design, replication and statistic.
        summary (List[Dict]): One row per design and statistic.
        comparisons (List[Dict]): One row per compared pair, point and measure.
        checks (List[Dict]): One row per check, design and bound.
        observed (List[Dict]): Each observed value beside its simulated interval.
        report (str): The Markdown report.
    """

    spec: StudySpec
    output: Path
    designs: List[Design]
    results: List[Dict[str, Any]]
    summary: List[Dict[str, Any]]
    comparisons: List[Dict[str, Any]]
    checks: List[Dict[str, Any]]
    observed: List[Dict[str, Any]]
    report: str

    @property
    def failed(self) -> List[Dict[str, Any]]:
        """The check rows that failed."""
        return [row for row in self.checks if not row["passed"]]


class CheckFailed(Exception):
    """
    A study's check failed. The artifacts are written before it is raised.

    Attributes:
        result (StudyResult): The whole result, failed checks included.
    """

    def __init__(self, result: StudyResult):
        self.result = result
        names = "; ".join(
            f"{row['design']}: {row['measure']} {row['bound']} {row['limit']:g} "
            f"(checked {row['value']:.4g})"
            for row in result.failed
        )
        count = len(result.failed)
        super().__init__(
            f"{'a check' if count == 1 else f'{count} checks'} failed: {names}"
        )
