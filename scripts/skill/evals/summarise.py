"""Makes the result table of skill-lessons.md, Evaluation, Result, from the JSON lines of all runs.

Usage:

    python summarise.py --grades GRADES.jsonl [...] --reviews REVIEWS.jsonl [...]
        [--cases-dir DIR]

`--grades` takes files of `grade.py` lines (run with `--version`, `--model` and `--run`), and
`--reviews` files of the blind reviewer's lines (`review_rubric.md`), to which the main session
has added `version`, `model` and `run`. Both have the fields `task`, `kind`, `count`,
`version`, `model`, `run`. A kind counts at most once per run and task, as skill-lessons.md,
Evaluation, Scoring, sets out: a `count` above 0 on any line of that run, task and kind, from
the script or the reviewer, scores 1.

A run with a line missing would read as a run with no mistake, and a run graded twice may hold
the lines of two gradings, so the runs to expect are not read from the lines. For each version
that the lines hold, every model of that version in skill-lessons.md, Evaluation, Runs, has
every task (each folder of `--cases-dir`, by default `cases/`; a line's `task` is that folder's name, such as `ds-a3`) for each of its run numbers:
Sonnet runs 1 to 3 of `before`, `after` and `none` (the baseline, the arm without the skill of
the `before` suite), Haiku and Opus run 1 of `before` and `after`, and for the release check of
`rc3`, Haiku runs 1 to 3 and Sonnet and Opus run 1. Each run has exactly one
line of each kind `grade.py` writes, and exactly one `{"kind": "REVIEWED", "count": 0}` line
of the reviewer, which may write any number of lines of its own kinds. The script lists each
run that lacks a line, holds a line twice, or is not in that list, and each reviewer line of a
kind the reviewer does not judge, and exits 1.

The output is a markdown table per model and version: the mean number of mistakes per task, per
kind, with the spread (the largest minus the smallest of the per-run means) for the model that
has several runs of a task; then the total, whose spread is that of the per-run totals; then
BASE (runs whose own tests fail on their own model), which is not a kind and is not in the
total. At the end it compares `before` with `after` for Sonnet and states whether the change is
kept: the total mean falls by half or more, and no kind rises by more than its spread, where a
kind's spread is the larger of its before and after spreads. A kind of either kind of scoring
(script or reviewer) that does not fall is listed, because its rule is rewritten.
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Set, Tuple

HERE = Path(__file__).resolve().parent

KINDS = [
    "K1",
    "K2",
    "K3",
    "K4",
    "K5",
    "K6",
    "K7",
    "K8",
    "K9",
    "K10",
    "K11",
    "K12",
    "K13",
    "K14",
    "K15",
    "K17",
]
# Why: BASE is reported beside the kinds, never in the total
ROWS = KINDS + ["BASE"]
# The lines `grade.py` writes once for every run (its `KINDS`)
GRADE_LINES = ["K1", "K2", "K4", "K6", "K7", "K9", "K11", "K12", "K13", "BASE"]
# The kinds the reviewer judges, and the line it writes once for every run
REVIEW_KINDS = ["K3", "K5", "K8", "K9", "K10", "K14", "K15", "K17"]
REVIEWED = "REVIEWED"
# The run numbers of each model and version (skill-lessons.md, Evaluation, Runs)
RUNS = {
    ("sonnet", "before"): ["1", "2", "3"],
    ("sonnet", "after"): ["1", "2", "3"],
    ("sonnet", "none"): ["1", "2", "3"],
    ("haiku", "before"): ["1"],
    ("haiku", "after"): ["1"],
    ("opus", "before"): ["1"],
    ("opus", "after"): ["1"],
    ("haiku", "rc3"): ["1", "2", "3"],
    ("sonnet", "rc3"): ["1"],
    ("opus", "rc3"): ["1"],
    ("haiku", "rc4"): ["1", "2", "3"],
}

RunKey = Tuple[str, str, str, str]


class Stat(NamedTuple):
    """The mean over runs of the per-run mean per task, and the spread of those per-run means."""

    mean: float
    spread: float


def load(paths: List[str]) -> List[dict]:
    """Reads the JSON lines of the given files.

    Args:
        paths: Files of `grade.py` and reviewer lines.

    Returns:
        Every line that is a JSON object.
    """
    rows = []
    for path in paths:
        for line in Path(path).read_text("utf-8").splitlines():
            line = line.strip()
            if line.startswith("{"):
                rows.append(json.loads(line))
    return rows


def _per_run(rows: List[dict]) -> Dict[RunKey, Dict[str, int]]:
    """Returns the mistakes per kind, 0 or 1, of each (model, version, task, run)."""
    runs: Dict[RunKey, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        counts = runs[
            _run_key(row)
        ]  # a run with only unscored lines still counts as a run
        if row["kind"] in ROWS:
            # A mistake counts once per task and kind (skill-lessons.md, Evaluation, Scoring)
            counts[row["kind"]] = max(counts[row["kind"]], min(1, int(row["count"])))
    return runs


class Gap(NamedTuple):
    """A run whose lines are wrong: what it lacks, holds twice, or why it is not a run."""

    run: RunKey
    lacks: List[str]


def _run_key(row: dict) -> RunKey:
    """Returns the (model, version, task, run) of a line."""
    return (
        row.get("model", ""),
        row.get("version", ""),
        row["task"],
        str(row.get("run", "")),
    )


def _kinds_seen(rows: List[dict]) -> Dict[RunKey, Dict[str, int]]:
    """Returns how many lines of each kind each run has."""
    seen: Dict[RunKey, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        seen[_run_key(row)][row["kind"]] += 1
    return seen


def _run_problems(grades: Dict[str, int], reviews: Dict[str, int]) -> List[str]:
    """Lists what is wrong with the lines of one run."""
    problems = []
    for kind in GRADE_LINES:
        if grades.get(kind, 0) == 0:
            problems.append(kind)
        elif grades[kind] > 1:
            problems.append(f"{kind} graded {grades[kind]} times")
    for kind in sorted(set(grades) - set(GRADE_LINES)):
        problems.append(f"{kind} is not a line of grade.py")
    if reviews.get(REVIEWED, 0) == 0:
        problems.append(REVIEWED)
    elif reviews[REVIEWED] > 1:
        problems.append(f"{REVIEWED} {reviews[REVIEWED]} times, so reviewed twice")
    for kind in sorted(set(reviews) - set(REVIEW_KINDS) - {REVIEWED}):
        problems.append(f"{kind} is not a kind the reviewer judges")
    return problems


def expected_runs(versions: Set[str], tasks: List[str]) -> List[RunKey]:
    """Returns every run of skill-lessons.md, Evaluation, Runs, of the given versions.

    Args:
        versions: The versions the lines hold.
        tasks: The task names.

    Returns:
        Each (model, version, task, run) to expect.
    """
    runs = []
    for (model, version), numbers in sorted(RUNS.items()):
        if version not in versions:
            continue
        for task in tasks:
            for number in numbers:
                runs.append((model, version, task, number))
    return runs


def missing_runs(
    grades: List[dict], reviews: List[dict], tasks: List[str]
) -> List[Gap]:
    """Lists each run whose lines are missing or repeated, and each line of no expected run.

    Args:
        grades: The lines of `grade.py` of every run.
        reviews: The reviewer's lines of every run.
        tasks: The task names, the folder names of `cases/`.

    Returns:
        Each run with what it lacks or holds twice, or why it is not a run of the plan: a
        task that is not a folder name of `cases/`, or a run the plan does not have.
    """
    by_grade = _kinds_seen(grades)
    by_review = _kinds_seen(reviews)
    seen = set(by_grade) | set(by_review)
    versions = {key[1] for key in seen}
    expected = expected_runs(versions, tasks)
    gaps = []
    for key in expected:
        problems = _run_problems(by_grade.get(key, {}), by_review.get(key, {}))
        if problems:
            gaps.append(Gap(key, problems))
    for key in sorted(seen - set(expected)):
        if key[2] not in tasks:
            reason = f"task {key[2]} is not a folder name of cases/, such as {tasks[0]}"
        else:
            reason = "not a run of the plan"
        gaps.append(Gap(key, [reason]))
    return gaps


def _stat(values: List[float]) -> Stat:
    """Returns the mean and the spread of per-run values."""
    values = values or [0.0]
    return Stat(sum(values) / len(values), max(values) - min(values))


Summary = Dict[Tuple[str, str], Dict[str, Stat]]


class Tables(NamedTuple):
    """The markdown tables, and for each (model, version) the Stat of each row and of "total"."""

    text: str
    summary: Summary


def _stats(by_run: Dict[str, Dict[str, Dict[str, int]]], tasks: int) -> Dict[str, Stat]:
    """Returns the Stat of each row and of "total" from the counts of each run and task."""
    # mean over tasks of each run number, then the spread between run numbers
    means: Dict[str, List[float]] = {}
    for row in ROWS:
        means[row] = []
        for counts in by_run.values():
            means[row].append(sum(c.get(row, 0) for c in counts.values()) / tasks)
    totals = [sum(means[kind][i] for kind in KINDS) for i in range(len(by_run))]
    stats = {row: _stat(values) for row, values in means.items()}
    stats["total"] = _stat(totals)
    return stats


def _lines(title: str, stats: Dict[str, Stat]) -> List[str]:
    """Returns the markdown table of one model and version."""
    lines = [
        title,
        "",
        "| Kind | Mean per task | Spread of the run means |",
        "|---|---|---|",
    ]
    for kind in KINDS:
        lines.append(f"| {kind} | {stats[kind].mean:.2f} | {stats[kind].spread:.2f} |")
    total, base = stats["total"], stats["BASE"]
    lines += [
        f"| Total | {total.mean:.2f} | {total.spread:.2f} |",
        f"| BASE (not in the total) | {base.mean:.2f} | {base.spread:.2f} |",
        "",
    ]
    return lines


def table(rows: List[dict]) -> Tables:
    """Makes the table of each model and version.

    Args:
        rows: The lines of every run, with no run missing.

    Returns:
        The markdown text and the Stat of each row of each model and version.
    """
    groups: Dict[Tuple[str, str], Dict[str, Dict[str, Dict[str, int]]]] = defaultdict(
        dict
    )
    for (model, version, task, run), counts in _per_run(rows).items():
        groups[(model, version)].setdefault(run, {})[task] = counts
    out_lines = []
    summary: Summary = {}
    for (model, version), by_run in sorted(groups.items()):
        tasks: Set[str] = set()
        for per_task in by_run.values():
            tasks |= set(per_task)
        stats = _stats(by_run, len(tasks))
        summary[(model, version)] = stats
        title = f"### {model or 'model'}, {version or 'version'} ({len(tasks)} tasks)"
        out_lines += _lines(title, stats)
    return Tables("\n".join(out_lines), summary)


def verdict(summary: Summary, model: str = "sonnet") -> str:
    """States whether the change is kept, from the before and after tables of one model.

    Args:
        summary: The `summary` of what `table` returns.
        model: The model whose before and after runs decide.

    Returns:
        The lines of the verdict.
    """
    before, after = summary.get((model, "before")), summary.get((model, "after"))
    if not before or not after:
        return f"No before and after runs for {model}, so no verdict."
    fell = after["total"].mean <= before["total"].mean / 2
    risen = []
    flat = []
    for kind in KINDS:
        b, a = before[kind], after[kind]
        spread = max(b.spread, a.spread)
        if a.mean > b.mean + spread:
            risen.append(f"{kind} ({b.mean:.2f} to {a.mean:.2f}, spread {spread:.2f})")
        if b.mean > 0 and a.mean >= b.mean:
            flat.append(kind)
    return "\n".join(
        [
            f"Total mean {before['total'].mean:.2f} before, {after['total'].mean:.2f} after: "
            f"{'a fall of half or more' if fell else 'less than half'}.",
            "Kinds that rose by more than their spread: "
            + (", ".join(risen) or "none")
            + ".",
            "Kinds that did not fall: "
            + (", ".join(flat) or "none")
            + " (their rules are rewritten and the set runs again).",
            f"The change is {'kept' if fell and not risen else 'not kept yet'}.",
        ]
    )


def main(argv: Optional[List[str]] = None) -> int:
    """Prints the tables and the verdict.

    Args:
        argv: The command line without the program name; None reads `sys.argv`.

    Returns:
        0, or 1 when a run lacks a line, holds one twice, or is not a run of the plan.
    """
    parser = argparse.ArgumentParser(description="Makes the result table.")
    parser.add_argument("--grades", nargs="+", type=Path, required=True)
    parser.add_argument("--reviews", nargs="+", type=Path, required=True)
    parser.add_argument("--cases-dir", type=Path, default=HERE / "cases")
    args = parser.parse_args(argv)
    grades = load([str(path) for path in args.grades])
    reviews = load([str(path) for path in args.reviews])
    tasks = []
    for folder in sorted(args.cases_dir.iterdir()):
        if folder.is_dir():
            tasks.append(folder.name)
    gaps = missing_runs(grades, reviews, tasks)
    rows = grades + reviews
    if gaps:
        print(
            "Runs whose lines are wrong (model, version, task, run: what is wrong):",
            file=sys.stderr,
        )
        for gap in gaps:
            print(f"  {', '.join(gap.run)}: {', '.join(gap.lacks)}", file=sys.stderr)
        return 1
    tables = table(rows)
    print(tables.text)
    print(verdict(tables.summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
