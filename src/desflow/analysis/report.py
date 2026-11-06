"""The experiment report: a Markdown file built only from the experiment's saved
artifacts.

`run_experiment` writes the artifacts and then this report, and `desflow report` builds
it again from the same files, so the two never differ. Every section says so when
it has nothing to report, rather than inventing content.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence, Tuple, Union

from desflow.analysis.results import (
    CHECKS_FILE,
    COMPARISONS_FILE,
    REPORT_FILE,
    EXPERIMENT_FILE,
    SUMMARY_FILE,
    read_csv,
    read_json,
)

_FORMS = {
    "yaml": "YAML blueprint",
    "builder": "builder factory",
    "low-level": "low-level function",
}


def _number(value: Any) -> str:
    """A value with four significant digits, as a reader compares them."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if number.is_integer() and abs(number) < 1e15:
        return str(int(number))
    return f"{number:.4g}"


def _table(heading: Sequence[str], rows: Sequence[Sequence[Any]]) -> List[str]:
    lines = ["| " + " | ".join(heading) + " |", "|" + "---|" * len(heading)]
    lines += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return lines


def _items(values: Sequence[str], empty: str) -> List[str]:
    return [f"- {value}" for value in values] if values else [empty]


def _interval(row: Dict[str, str]) -> str:
    return f"{_number(row['low'])} to {_number(row['high'])}"


def _distributions(experiment: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {
        path: values
        for path, values in experiment["parameters"].items()
        if isinstance(values, dict) and "dist" in values
    }


def _end_setting(value: Any, unset: str) -> str:
    """A stop or drain setting as the report shows it; unset, the model's own."""
    if value is None:
        return f"{unset} unless the model's own run sets one"
    if value is True:
        return "yes"
    if value is False:
        return "no"
    return f"`{value}`"


def _missing_inputs(experiment: Dict[str, Any]) -> List[str]:
    """What the experiment does not state: each is named, never filled in."""
    missing = []
    if not experiment.get("question"):
        missing.append("The question the experiment answers is not stated.")
    if not experiment.get("assumptions"):
        missing.append("No assumptions are stated.")
    for path in _distributions(experiment):
        if path not in experiment.get("inputs", {}):
            missing.append(f"The provenance of `{path}` is not stated.")
    if not experiment.get("observed"):
        missing.append(
            "No observed values are given, so the model's output is not compared "
            "with the real system."
        )
    if not experiment.get("limits"):
        missing.append("No limits are stated.")
    return missing


@dataclass(frozen=True)
class _Artifacts:
    """What the report reads from an experiment's saved artifacts."""

    experiment: Dict[str, Any]
    settings: Dict[str, Any]
    summary: List[Dict[str, str]]
    comparisons: List[Dict[str, str]]
    checks: List[Dict[str, str]]
    measures: List[str]
    counts: List[str]
    designs: List[str]
    model: Dict[str, Any]
    versions: Dict[str, str]


def _artifacts(folder: Path) -> _Artifacts:
    """Reads the artifacts in `folder` that the report is built from."""
    experiment = read_json(folder / EXPERIMENT_FILE)
    settings = experiment["settings"]
    return _Artifacts(
        experiment=experiment,
        settings=settings,
        summary=read_csv(folder / SUMMARY_FILE),
        comparisons=read_csv(folder / COMPARISONS_FILE),
        checks=read_csv(folder / CHECKS_FILE),
        measures=settings["measures"],
        counts=experiment["counts"],
        designs=[d["name"] for d in experiment["designs"]],
        model=experiment["model"],
        versions=experiment["versions"],
    )


def _question(artifacts: _Artifacts) -> List[str]:
    """The lines of the Question section, below its heading."""
    return [artifacts.experiment.get("question") or "Not stated."]


def _model(artifacts: _Artifacts) -> List[str]:
    """The lines of the Model section, below its heading."""
    model = artifacts.model
    return [
        f"- Source: `{model['source']}`, a {_FORMS[model['form']]}"
        + (f", SHA-256 `{model['sha256']}`" if model.get("sha256") else ""),
        f"- Sim id: `{model['sim_id']}`",
        "- Versions: "
        + ", ".join(
            f"{name} {version}" for name, version in artifacts.versions.items()
        ),
    ]


def _assumptions(artifacts: _Artifacts) -> List[str]:
    """The lines of the Assumptions section, below its heading."""
    return _items(artifacts.experiment.get("assumptions", []), "None stated.")


def _inputs(artifacts: _Artifacts) -> List[str]:
    """The lines of the Inputs and Provenance section, below its heading."""
    experiment = artifacts.experiment
    lines: List[str] = []
    distributions = _distributions(experiment)
    if distributions:
        rows = []
        for path, values in distributions.items():
            parameters = ", ".join(f"{k}={v}" for k, v in values.items() if k != "dist")
            provenance = experiment.get("inputs", {}).get(path, "not stated")
            rows.append([f"`{path}`", values["dist"], parameters, provenance])
        lines += _table(["Input", "Family", "Parameters", "Provenance"], rows)
    else:
        lines.append("The model registers no distributions.")
    return lines


def _run_settings(artifacts: _Artifacts) -> List[str]:
    """The lines of the Run Settings section, below its heading."""
    experiment = artifacts.experiment
    settings = artifacts.settings
    lines: List[str] = []
    warmup = settings.get("warmup") or 0
    horizon = settings["until"] - warmup
    pairing = (
        "common: every design runs with the same seeds"
        if settings["pairing"] == "common"
        else "independent: each design runs with seeds of its own"
    )
    lines += _table(
        ["Setting", "Value"],
        [
            ["Until, where arrivals stop", f"{_number(settings['until'])} s"],
            ["Stop", _end_setting(settings.get("stop"), "none")],
            ["Drain", _end_setting(settings.get("drain"), "no")],
            ["Warm-up", f"{_number(warmup)} s"],
            [
                "Measured horizon",
                f"{_number(horizon)} s, until minus the warm-up, for a run that "
                "ends at until",
            ],
            ["Replications", f"{settings['replications']} per design"],
            [
                "Seed",
                f"{settings['seed']}, which the replications' seeds are spawned from",
            ],
            ["Pairing", pairing],
            ["Confidence level", f"{settings['level'] * 100:g}%"],
        ],
    )
    lines += [""]
    lines += _table(
        ["Design", "Registry values it sets"],
        [
            [
                d["name"],
                ", ".join(f"`{k}` = {v}" for k, v in d["overrides"].items())
                or "none: the model as written",
            ]
            for d in experiment["designs"]
        ],
    )
    return lines


def _counts(artifacts: _Artifacts) -> List[str]:
    """The lines of the Counts section, below its heading."""
    summary = artifacts.summary
    counts = artifacts.counts
    designs = artifacts.designs
    lines: List[str] = []
    if counts:
        lines.append(
            "The mean over the replications of each count the statistics publish. "
            "`started` and `finished` count from the warm-up to the end of the run: "
            "`until`, the stop, or the end of the drain; `in_system` "
            "and `in_progress` are read at the end of the run, the entities and "
            "task instances still in the model, which finished counts leave out. "
            "Without a warm-up, `started` is `finished` plus `in_system` plus the "
            "entities that balked, reneged, failed or were abandoned."
        )
        lines.append("")
        means = {(r["design"], r["statistic"]): r["mean"] for r in summary}
        rows = []
        for design in designs:
            rows.append([design, *[_number(means[design, name]) for name in counts]])
        lines += _table(["Design", *(f"`{c}`" for c in counts)], rows)
    else:
        lines.append("The statistics published no counts of tasks or flows.")
    return lines


def _results(artifacts: _Artifacts) -> List[str]:
    """The lines of the Results section, below its heading."""
    settings = artifacts.settings
    summary = artifacts.summary
    measures = artifacts.measures
    lines: List[str] = []
    lines.append(
        f"Each measure's mean over the replications, with its {settings['level'] * 100:g}% "
        "confidence interval."
    )
    lines.append("")
    lines += _table(
        ["Design", "Measure", "Population", "Mean", "Half-width", "Interval", "n"],
        [
            [
                r["design"],
                f"`{r['statistic']}`",
                r.get("population", ""),
                _number(r["mean"]),
                _number(r["half_width"]),
                _interval(r),
                r["n"],
            ]
            for r in summary
            if r["statistic"] in measures
        ],
    )
    return lines


def _comparisons(artifacts: _Artifacts) -> List[str]:
    """The lines of the Comparisons section, below its heading."""
    comparisons = artifacts.comparisons
    lines: List[str] = []
    if comparisons:
        lines.append(
            "Each difference is the variant's value minus the baseline's, seed by "
            "seed, with the confidence interval of the mean difference."
        )
        lines.append("")
        rows = []
        for r in comparisons:
            only = [
                f"only the baseline: {r['streams_baseline_only']}"
                if r["streams_baseline_only"]
                else "",
                f"only the variant: {r['streams_variant_only']}"
                if r["streams_variant_only"]
                else "",
            ]
            only = [o for o in only if o]
            both = len([s for s in r["streams_both"].split(";") if s])
            streams = f"{both} shared" + (f"; {'; '.join(only)}" if only else "")
            rows.append(
                [
                    r["baseline"],
                    r["variant"],
                    f"`{r['measure']}`",
                    _number(r["difference"]),
                    _number(r["half_width"]),
                    _interval(r),
                    streams,
                ]
            )
        lines += _table(
            [
                "Baseline",
                "Variant",
                "Measure",
                "Difference",
                "Half-width",
                "Interval",
                "Random streams",
            ],
            rows,
        )
    else:
        lines.append("No comparisons were requested.")
    return lines


def _checks(artifacts: _Artifacts) -> List[str]:
    """The lines of the Checks section, below its heading."""
    checks = artifacts.checks
    lines: List[str] = []
    if checks:
        passed = len([r for r in checks if r["passed"] == "True"])
        lines.append(f"Checks passed: {passed} of {len(checks)}.")
        lines.append("")
        lines += _table(
            ["Design", "Measure", "Checked", "Bound", "Value", "Result"],
            [
                [
                    r["design"],
                    f"`{r['measure']}`",
                    "the mean"
                    if r["on"] == "mean"
                    else ("the upper end" if r["bound"] == "max" else "the lower end"),
                    f"{r['bound']} {_number(r['limit'])}",
                    _number(r["value"]),
                    "passed" if r["passed"] == "True" else "failed",
                ]
                for r in checks
            ],
        )
    else:
        lines.append("No checks were requested.")
    return lines


def _observed(artifacts: _Artifacts) -> List[str]:
    """The lines of the Observed Comparison section, below its heading."""
    experiment = artifacts.experiment
    lines: List[str] = []
    observed = experiment.get("observed") or []
    if observed:
        lines += _table(
            ["Design", "Measure", "Observed", "Unit", "Simulated interval", "Inside"],
            [
                [
                    r["design"],
                    f"`{r['measure']}`",
                    _number(r["value"]),
                    r["unit"],
                    _interval(r),
                    "yes" if r["inside"] else "no",
                ]
                for r in observed
            ],
        )
    else:
        lines.append("No observed values were given.")
    return lines


def _limits(artifacts: _Artifacts) -> List[str]:
    """The lines of the Limits section, below its heading."""
    return _items(artifacts.experiment.get("limits", []), "None stated.")


def _missing(artifacts: _Artifacts) -> List[str]:
    """The lines of the Missing Inputs section, below its heading."""
    return _items(_missing_inputs(artifacts.experiment), "None.")


# The report's sections, in order, each with the function that gives its lines
_SECTIONS: Tuple[Tuple[str, Callable[[_Artifacts], List[str]]], ...] = (
    ("Question", _question),
    ("Model", _model),
    ("Assumptions", _assumptions),
    ("Inputs and Provenance", _inputs),
    ("Run Settings", _run_settings),
    ("Counts", _counts),
    ("Results", _results),
    ("Comparisons", _comparisons),
    ("Checks", _checks),
    ("Observed Comparison", _observed),
    ("Limits", _limits),
    ("Missing Inputs", _missing),
)


def build_report(folder: Union[str, Path]) -> str:
    """
    Builds the report of the experiment whose artifacts are in `folder`.

    Args:
        folder: The experiment's output folder, holding `experiment.json` and the
            CSV files.

    Returns:
        str: The report, as Markdown.

    Raises:
        FileNotFoundError: If the folder has no `experiment.json`.
    """
    artifacts = _artifacts(Path(folder))
    lines = [f"# Experiment: {artifacts.experiment['experiment']}"]
    for heading, section in _SECTIONS:
        lines += ["", f"## {heading}", "", *section(artifacts)]
    return "\n".join(lines) + "\n"


def write_report(folder: Union[str, Path]) -> Path:
    """
    Builds the report from the experiment's artifacts and writes it beside them.

    Args:
        folder: The experiment's output folder.

    Returns:
        Path: The `report.md` written.
    """
    path = Path(folder) / REPORT_FILE
    path.write_text(build_report(folder), encoding="utf-8")
    return path
