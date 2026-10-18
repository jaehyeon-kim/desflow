"""The study report: a Markdown file built from the study's saved artifacts only.

`run_study` writes the artifacts and then this report, and `desflow report` builds
it again from the same files, so the two never differ. Every section says so when
it has nothing to report, rather than inventing content.
"""

from pathlib import Path
from typing import Any, Dict, List, Sequence, Union

from desflow.study.results import (
    CHECKS_FILE,
    COMPARISONS_FILE,
    REPORT_FILE,
    RESULTS_FILE,
    STUDY_FILE,
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


def _distributions(study: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {
        path: values
        for path, values in study["parameters"].items()
        if isinstance(values, dict) and "dist" in values
    }


def _missing_inputs(study: Dict[str, Any]) -> List[str]:
    """What the study does not state: each is named, never filled in."""
    missing = []
    if not study.get("question"):
        missing.append("The question the study answers is not stated.")
    if not study.get("assumptions"):
        missing.append("No assumptions are stated.")
    for path in _distributions(study):
        if path not in study.get("inputs", {}):
            missing.append(f"The provenance of `{path}` is not stated.")
    if not study.get("observed"):
        missing.append(
            "No observed values are given, so the model's output is not compared "
            "with the real system."
        )
    if not study.get("limits"):
        missing.append("No limits are stated.")
    return missing


def build_report(folder: Union[str, Path]) -> str:
    """
    Builds the report of the study whose artifacts are in `folder`.

    Args:
        folder: The study's output folder, holding `study.json` and the CSV files.

    Returns:
        str: The report, as Markdown.

    Raises:
        FileNotFoundError: If the folder has no `study.json`.
    """
    folder = Path(folder)
    study = read_json(folder / STUDY_FILE)
    settings = study["settings"]
    summary = read_csv(folder / SUMMARY_FILE)
    results = read_csv(folder / RESULTS_FILE)
    comparisons = read_csv(folder / COMPARISONS_FILE)
    checks = read_csv(folder / CHECKS_FILE)
    measures = settings["measures"]
    counts = study["counts"]
    designs = [d["name"] for d in study["designs"]]
    model = study["model"]
    versions = study["versions"]

    lines = [f"# Study: {study['study']}", "", "## Question", ""]
    lines.append(study.get("question") or "Not stated.")

    lines += ["", "## Model", ""]
    lines += [
        f"- Source: `{model['source']}`, a {_FORMS[model['form']]}"
        + (f", SHA-256 `{model['sha256']}`" if model.get("sha256") else ""),
        f"- Sim id: `{model['sim_id']}`",
        "- Versions: "
        + ", ".join(f"{name} {version}" for name, version in versions.items()),
    ]

    lines += ["", "## Assumptions", ""]
    lines += _items(study.get("assumptions", []), "None stated.")

    lines += ["", "## Inputs and Provenance", ""]
    distributions = _distributions(study)
    if distributions:
        rows = []
        for path, values in distributions.items():
            parameters = ", ".join(f"{k}={v}" for k, v in values.items() if k != "dist")
            provenance = study.get("inputs", {}).get(path, "not stated")
            rows.append([f"`{path}`", values["dist"], parameters, provenance])
        lines += _table(["Input", "Family", "Parameters", "Provenance"], rows)
    else:
        lines.append("The model registers no distributions.")

    lines += ["", "## Run Settings", ""]
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
            ["End time", f"{_number(settings['until'])} s"],
            ["Warm-up", f"{_number(warmup)} s"],
            [
                "Measured horizon",
                f"{_number(horizon)} s, the end time minus the warm-up",
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
            for d in study["designs"]
        ],
    )

    lines += ["", "## Counts", ""]
    if counts:
        lines.append(
            "The mean over the replications of each count the statistics publish. "
            "`started` and `finished` count over the measured horizon; `in_system` "
            "and `in_progress` are read at the end of the run, the entities and "
            "task instances still in the model, which finished counts leave out. "
            "Without a warm-up, `started` is `finished` plus `in_system` plus the "
            "entities that balked, reneged, failed or were abandoned."
        )
        lines.append("")
        rows = []
        for design in designs:
            row = [design]
            for name in counts:
                series = [
                    float(r["value"])
                    for r in results
                    if r["design"] == design and r["statistic"] == name
                ]
                row.append(_number(sum(series) / len(series)))
            rows.append(row)
        lines += _table(["Design", *(f"`{c}`" for c in counts)], rows)
    else:
        lines.append("The statistics published no counts of tasks or flows.")

    lines += ["", "## Results", ""]
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

    lines += ["", "## Comparisons", ""]
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

    lines += ["", "## Checks", ""]
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

    lines += ["", "## Observed Comparison", ""]
    observed = study.get("observed") or []
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

    lines += ["", "## Limits", ""]
    lines += _items(study.get("limits", []), "None stated.")

    lines += ["", "## Missing Inputs", ""]
    lines += _items(_missing_inputs(study), "None.")
    return "\n".join(lines) + "\n"


def write_report(folder: Union[str, Path]) -> Path:
    """
    Builds the report from the study's artifacts and writes it beside them.

    Args:
        folder: The study's output folder.

    Returns:
        Path: The `report.md` written.
    """
    path = Path(folder) / REPORT_FILE
    path.write_text(build_report(folder), encoding="utf-8")
    return path
