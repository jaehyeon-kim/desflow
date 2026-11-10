"""Scores one kept evaluation run by script, as skill-lessons.md, Evaluation, sets out.

Usage:

    python grade.py ROOT --task p33 --python-dir DIR [--check-python-dir DIR] [--skill DIR]
        [--other-skill DIR] [--api-page FILE] [--records-out DIR] [--version before]
        [--model sonnet] [--run 1]

`ROOT` is a run folder that `claude plugin eval --keep-temp` kept (`/tmp/e-XXXXXX` on macOS):
the agent's workspace is `ROOT/sealed/home/cwd` and its full transcript `ROOT/out/trace.jsonl`.
The command leaves both unreadable (mode 500 and 000), so `chmod 700 ROOT ROOT/sealed` comes
first; the script exits 2 when the workspace cannot be read. `--task` is the case's name, a
folder name of `cases/` such as `ds-a3`; its folder of `tasks/` writes each `-` as `_`.
`--python-dir` is the prepared Python of the run's version (`make_suite.py`): base desflow
from the wheel, pytest and pytest-xdist; the forms and the tests run with it.
`--check-python-dir` (by default `--python-dir`) is a prepared Python of the repository's
current tree, which runs `scripts/check_model.py` of that tree for every run, whatever its
version: the run child that sets a Python form's seed through desflow and records the seeds
other code sets, and `desflow run --seed` for the YAML at a set seed. The script runs
everything in copies of the workspace, without the run's own `py/`, so it writes nothing into
the kept folder. K6 is judged against `--api-page`, by default `references/api.md` of
`--skill`: an import is public when that page names its module in `from desflow.x import ...`.
A run without the skill names the page of the version it is compared with.

`--records-out` names a folder where the script writes what the blind reviewer reads
(`review_rubric.md`, What the reviewer gets): the run's files, the transcript, the records of
each form as this script runs them at the file's own seed, `tests.json`, `clocks.json`, the
task's prompt and records, the original program of the known case, and the pages of the skill
copy `--skill` that `--other-skill` also has. In all of them the path of `--skill` becomes
`<skill>`, each page that only one of `--skill` and `--other-skill` has becomes `page-1.md`,
`page-2.md` and so on, a suite folder under the p12e3 scratch folder becomes `<suite>`, and a
kept run folder (`/private/tmp/e-XXXXXX`) becomes `<run>` (`grade_facts.py`).

The output is one JSON line per kind: `{"task", "kind", "count", "evidence", ...}`, where
`evidence` is a list of short texts with a file and line, or the change that no test caught,
and `notes`, when present, says what was not counted and why. The kinds are K1 K2 K4 K6 K7 K9
K11 K12 K13, and BASE (the agent's own tests that fail on the agent's own, unchanged model in
each of three plain runs), which is not a kind of the plan. The tests also run three times
under `pytest -n 4` and three times in reverse order, and the tests of each changed model run
in the same three ways (`grade_tests.py`); each outcome is read from pytest's own reports
(`grade_outcomes.py`). A test whose outcome differs within one of these ways is flaky: it
counts for neither BASE nor K12, it is not a test that passes or fails for K9, and
`tests.json` gives its outcomes for the reviewer. K3, K5, K8, K10, K14, K15, K17, and K9 where
a test fails under a change, are judged by the reviewer from the facts this script writes.

What each kind counts. A mistake counts once per task and kind in `summarise.py`, however
often it repeats here.

- K1: a `varies` entry of `records.yaml` whose records are the same at three seeds.
- K2: a record of `records.yaml` that a form lacks (one count per form and record), a form
  whose file is missing, and a paced YAML form that prints its records faster than
  `wall_min`. A Python form that cannot run is a K11 mistake and is not scored again.
- K4: a pair of forms (yaml/builder, yaml/low_level, builder/low_level) whose event records
  differ at any of three seeds: each file's own seed, and the two after the seed of
  `model.yaml`, set through desflow (`desflow run --seed` for the YAML, the run child of
  `check_model.py` for a Python form, which replaces the seed of each context run and each
  `Sampler`), so how a file writes its seed does not matter. A model with no literal
  `random_seed` is compared at its own seeds only, and the notes say so. K4 counts only
  records that differ.
- K6: Builder, Low-level or test code that imports a desflow module the API page does not
  name: `from desflow.x import y` or `import desflow.x` for a module other than the top level
  and those `--api-page` names, an imported name that starts with one underscore,
  `from desflow import x` where `x` is an undocumented submodule, or a submodule reached
  through an imported documented module (`values.runtime.f`). Reading a private attribute of
  a desflow object is not K6; the reviewer judges it under K5 or K14.
- K7: a run end in `model.yaml` that differs from `run_end` of `records.yaml`, and a time that
  the request gives in minutes, hours or days (`unit_times` of `records.yaml`) written as a
  bare number in `model.yaml` or the Builder file, or as a Low-level literal in a call with no
  comment on its lines. No task of this evaluation has `unit_times`, so here K7 counts only
  the run end. A bare number for a time the request gives in seconds is not a mistake,
  because desflow reads it as seconds; the notes list such numbers as evidence only. A time
  whose value in seconds is wrong changes the records, so K2 counts it, not K7.
- K9: a change of `mutations.yaml`, applied to `model.yaml`, under which no steady test that
  passes on the agent's model fails steadily. Each change tries its listed keys in order and
  keeps the first that alters the records; it is made on PyYAML's node tree, so tags such as
  `!python` stay as written. A change after which the model does not run, or that leaves the
  records the same, is not counted. Every test runs on each changed model three times in each
  of the three ways, and `tests.json` holds each outcome. When some test fails under a change, the notes name those tests and the
  reviewer judges whether one of them checks the behaviour the change is about. When no test
  passes on the agent's model, every change counts.
- K11: a Python form (`builder.py`, `low_level.py`) that does not exit 0 under `python FILE`
  with only base desflow, or prints no record as a JSON line.
- K12: a test, not flaky, whose outcome in the plain runs, under `pytest -n 4` and in reverse
  order is not the same.
- K13: a draw in `low_level.py` that desflow does not seed, as `check_model.py` reports
  it through its run child: two runs at the file's own seed that differ (a draw with no
  seed), and each place where the file's own code seeds a random generator that no
  `Sampler` was given (`_own_seeds`), once per place.

The facts for the reviewer. `tests.json` holds, for each test id, its outcome on the agent's
model (`unchanged`), its outcome under each applied change (`changes`), and the forms
`form_trace.py` saw it run (`forms`; its docstring says what counts as running a form), and
the list of changes with what each applied or why none was. `clocks.json` holds each assert of
the tests that reads a wall clock (`time.time`, `time.monotonic`, `time.perf_counter`, under
any name they are imported as, directly, through a name assigned from one, or through a module
function that returns one), with its line and text, and `paced_wall`, the wall seconds the
request's pacing gives (null for an unpaced task).

`records.yaml` fields. Top level: `task`, `until` (seconds, given to `desflow run --until` when
`model.yaml` has no run end of its own; null for none), `run_end` (the original's end in
seconds, or null), `via` (`sample`, the default, `stdout` or `run`: how the YAML form is run),
`wall_min` (the least wall seconds between the first and the last record line of a paced
`stdout` run), `paced_wall` (the wall seconds the request's pacing gives), `records`,
`varies`, `unit_times` (each time the request gives in minutes, hours or days: `name`, `value`
in that unit, `unit`). A record: `name`; `match` (a mapping from a field of the record, with
dots for nested fields such as `value.event_type`, to a wanted value: a scalar, a list meaning
any of, `re:` and a regular expression, or null, which also matches a missing field); `at`
(`sim_ts`); `between` (`[a, b]`: only records with a < `sim_ts` < b); `count` (a number, or
`{min, max}`; the default is at least 1); `every_multiple_of` (every match has a `sim_ts` that
is a multiple of it); `before` (names of records that come later in the stream); `forms` (the
default is all three).

`mutations.yaml`: each mutation has a `name` and `changes`; a change has `keys` (dotted paths
in `model.yaml`, where `*` stands for the one key there is, a number for a list index, and a
part in braces, such as `services.{tasks.job.service}.value`, for the text that the dotted path
inside the braces holds in `model.yaml`), `new`, and optionally `add: true` (when no listed key
exists, add the first whose parent exists) or `optional: true` (a change that may find no key).
"""

import argparse
import ast
import importlib.util
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterator, List, NamedTuple, Optional, Set, Tuple

import yaml

from grade_changes import ChangeRun, run_changes
from grade_clocks import clock_asserts
from grade_facts import Sources, hidden_names, tests_facts, write_facts
from grade_runs import (
    KINDS,
    Env,
    Score,
    FILES,
    FORMS,
    RUN_TIMEOUT,
    PythonFile,
    canonical,
    check_model,
    form_ran,
    stream_records,
    run_form,
    model_document,
    model_seed,
    run_seeded,
)
from grade_tests import FLAKY, Steady, outcome_of, run_steady
from grade_units import score_units


HERE = Path(__file__).resolve().parent


_MISSING = object()


# A module the API page of the skill names in `from desflow.x import ...` or `import desflow.x`
_DOCUMENTED = re.compile(
    r"`(?:from\s+(desflow(?:\.\w+)*)\s+import\b|import\s+(desflow(?:\.\w+)*))"
)


def get(record: dict, path: str) -> Any:
    """Returns the field at a dotted path, or `_MISSING`."""
    cur: Any = record
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return _MISSING
    return cur


def _same(actual: Any, want: Any) -> bool:
    """True when a field meets a wanted value of `match` (records.yaml fields)."""
    if isinstance(want, list):
        return any(_same(actual, w) for w in want)
    if want is None:
        return actual is None or actual is _MISSING
    if actual is _MISSING:
        return False
    if isinstance(want, str) and want.startswith("re:"):
        return re.search(want[3:], str(actual)) is not None
    if isinstance(want, bool) or isinstance(actual, bool):
        return actual is want
    if isinstance(want, (int, float)) and isinstance(actual, (int, float)):
        return abs(actual - want) < 1e-6
    return bool(actual == want)


def _in_time(entry: dict, ts: Any) -> bool:
    """True when a record's `sim_ts` meets the entry's `at` and `between`."""
    number = isinstance(ts, (int, float))
    if "at" in entry and not (number and abs(ts - entry["at"]) < 1e-6):
        return False
    if "between" in entry:
        low, high = entry["between"]
        return number and low < ts < high
    return True


class _Match(NamedTuple):
    """A record that meets an entry of `records.yaml`, and its place in the stream."""

    place: int
    record: dict


def _meets(record: dict, entry: dict) -> bool:
    """True when a record has every field of an entry's `match` and meets its times."""
    fields = all(_same(get(record, k), v) for k, v in entry["match"].items())
    return fields and _in_time(entry, record.get("sim_ts"))


def _candidates(records: List[dict], entry: dict) -> List[_Match]:
    """Returns the records, with their place in the stream, that meet an entry."""
    found = []
    for index, record in enumerate(records):
        if _meets(record, entry):
            found.append(_Match(index, record))
    return found


def _off_multiple(times: List[float], step: float) -> List[float]:
    """Returns the times that are not multiples of `step`."""
    bad = []
    for ts in times:
        if abs(ts / step - round(ts / step)) > 1e-6:
            bad.append(ts)
    return bad


def _check_entry(
    records: List[dict], entry: dict, entries: Dict[str, dict]
) -> Optional[str]:
    """Returns None when the records meet the entry, else a short reason."""
    found = _candidates(records, entry)
    want = entry.get("count", {"min": 1})
    low, high = (
        (want, want) if isinstance(want, int) else (want.get("min", 0), want.get("max"))
    )
    if len(found) < low or (high is not None and len(found) > high):
        return f"{len(found)} records, wanted {want}"
    if "every_multiple_of" in entry:
        step = entry["every_multiple_of"]
        bad = _off_multiple([m.record.get("sim_ts", 0) for m in found], step)
        if bad:
            return f"records at {bad[:3]} are not multiples of {step}"
    for later in entry.get("before", []):
        other = _candidates(records, entries[later])
        if found and other and found[0].place > other[0].place:
            return f"comes after '{later}' in the stream"
    return None


def _score_records(env: Env, spec: dict, score: Score) -> None:
    """K2 and K11, from the runs of the three forms at the file's own seed."""
    entries = {e["name"]: e for e in spec["records"]}
    for form in FORMS:
        result = run_form(env, spec, form)
        if result is None:
            score.evidence["K2"].append(f"{FILES[form]} is missing")
            continue
        tail = " | ".join(result.stderr.strip().splitlines()[-2:])[:300]
        if not form_ran(form, result):
            kind = "K2" if form == "yaml" else "K11"
            score.evidence[kind].append(
                f"{FILES[form]}: exit {result.rc}, {len(result.records)} records. {tail}"
            )
            continue
        for entry in spec["records"]:
            if form not in entry.get("forms", FORMS):
                continue
            problem = _check_entry(result.records, entry, entries)
            if problem:
                score.evidence["K2"].append(
                    f"{FILES[form]}: '{entry['name']}': {problem}"
                )
        wall_min = spec.get("wall_min")
        if form == "yaml" and wall_min and result.wall < wall_min:
            score.evidence["K2"].append(
                f"model.yaml: the paced run printed its records over {result.wall:.2f} s, "
                f"wanted at least {wall_min}"
            )


def _first_difference(left: List[str], right: List[str]) -> str:
    """Describes the first record where two streams differ."""
    n = min(len(left), len(right))
    for index, (x, y) in enumerate(zip(left, right)):
        if x != y:
            n = index
            break
    lv = left[n] if n < len(left) else "(none)"
    rv = right[n] if n < len(right) else "(none)"
    return f"record {n}: {lv} / {rv}"


def _seeds(env: Env) -> List[Optional[int]]:
    """Returns the seeds the forms are compared at: their own, and the two after the model's.

    None stands for each file's own seeds. The two later seeds are those after
    `simulation.random_seed` of `model.yaml`; a model with no literal seed gives its own only.
    """
    base = model_seed(model_document(env.run_dir / "model.yaml"))
    return [None] if base is None else [None, base + 1, base + 2]


def _score_forms_agree(env: Env, spec: dict, score: Score) -> None:
    """K4: a pair of forms whose event records differ at one of three seeds.

    Each form runs at each seed through `run_seeded`, which sets the seed through desflow,
    so how a file writes its seed does not matter.
    """
    seeds = _seeds(env)
    if len(seeds) == 1:
        score.notes["K4"].append(
            "model.yaml has no literal random_seed, so the forms are compared at their own seeds only"
        )
    for a, b in (("yaml", "builder"), ("yaml", "low_level"), ("builder", "low_level")):
        for seed in seeds:
            left, right = run_seeded(env, spec, a, seed), run_seeded(env, spec, b, seed)
            if left is None or right is None:
                continue
            if not (form_ran(a, left) and form_ran(b, right)):
                continue
            ls, rs = canonical(left.records), canonical(right.records)
            if ls != rs:
                where = "their own seeds" if seed is None else f"seed {seed}"
                score.evidence["K4"].append(
                    f"{a} and {b} differ at {where}, {_first_difference(ls, rs)}"
                )
                break


def _score_varies(env: Env, spec: dict, score: Score) -> None:
    """K1: a `varies` entry whose records are the same at three seeds."""
    base = model_seed(model_document(env.run_dir / "model.yaml"))
    if base is None:
        if spec.get("varies"):
            score.notes["K1"].append("model.yaml has no literal random_seed to change")
        return
    for entry in spec.get("varies", []):
        fields = entry.get("project", ["sim_ts", "key"])
        shapes: List[Optional[List[Tuple[str, ...]]]] = []
        for seed in (base, base + 1, base + 2):
            result = run_seeded(env, spec, "yaml", seed)
            if result is None or result.rc != 0:
                shapes.append(None)
                continue
            found = _candidates(result.records, entry)
            shapes.append([tuple(str(get(m.record, f)) for f in fields) for m in found])
        if all(shapes) and shapes[0] == shapes[1] == shapes[2]:
            score.evidence["K1"].append(
                f"'{entry['name']}': the same {len(shapes[0] or [])} records at seeds "
                f"{base}, {base + 1}, {base + 2}"
            )


def _python_files(env: Env) -> Dict[str, PythonFile]:
    """Returns the text and syntax tree of each Python file of the run."""
    found = {}
    for name in ("builder.py", "low_level.py", "test_model.py"):
        path = env.run_dir / name
        if path.is_file():
            text = path.read_text("utf-8")
            try:
                found[name] = PythonFile(text, ast.parse(text))
            except SyntaxError as exc:
                found[name] = PythonFile(text, ast.parse(""))
                print(f"grade.py: {name} does not parse: {exc}", file=sys.stderr)
    return found


def _documented_modules(api_page: Path) -> Set[str]:
    """Returns the top level and each module the skill's API page names as one to import from.

    The page differs between versions of the skill (the version before the changes names only
    `from desflow import ...`), so K6 is judged against the page the run could read.
    """
    found = {"desflow"}
    for match in _DOCUMENTED.finditer(api_page.read_text("utf-8")):
        found.add(match.group(1) or match.group(2))
    return found


def _is_module(name: str) -> bool:
    """True when `name` is a module of desflow that can be imported."""
    try:
        return importlib.util.find_spec(name) is not None
    except (ImportError, AttributeError, ValueError):
        return False


def _dotted(node: ast.AST) -> Optional[List[str]]:
    """Returns the names of an attribute chain such as `a.b.c`, or None."""
    names: List[str] = []
    while isinstance(node, ast.Attribute):
        names.insert(0, node.attr)
        node = node.value
    if not isinstance(node, ast.Name):
        return None
    return [node.id] + names


def _private_imports(tree: ast.Module, modules: Set[str]) -> Iterator[Tuple[int, str]]:
    """Yields each import of desflow that the API page does not name, with its line.

    An import from a module the page does not name, of a name that starts with one
    underscore, or of a module the page does not name (`import desflow.x`, or
    `from desflow import x` when `x` is a module). A module reached by an attribute chain
    on a module the page names, such as `desflow.connectors.egress` after `import desflow`,
    counts as an import of it. An attribute that starts with an underscore is not judged
    here: the reviewer reads it (`review_rubric.md`, K5).
    """
    bound: Dict[str, str] = {}
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.ImportFrom)
            and (node.module or "").split(".")[0] == "desflow"
        ):
            module = node.module or ""
            if module not in modules:
                yield node.lineno, f"imports from {module}"
            for alias in node.names:
                full = f"{module}.{alias.name}"
                if alias.name.startswith("_") and not alias.name.startswith("__"):
                    yield node.lineno, f"imports {alias.name}"
                elif _is_module(full):
                    bound[alias.asname or alias.name] = full
                    if full not in modules:
                        yield node.lineno, f"imports {full}"
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] != "desflow":
                    continue
                if alias.asname:
                    bound[alias.asname] = alias.name
                else:
                    bound["desflow"] = "desflow"
                if alias.name not in modules:
                    yield node.lineno, f"imports {alias.name}"
    # Why: ast.walk meets the outer chain of `a.b.c` before its inner `a.b`, so one chain
    # is judged once, by its line and the name it starts from
    seen: Set[Tuple[int, str]] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Attribute):
            continue
        names = _dotted(node)
        if not names or names[0] not in bound or bound[names[0]] not in modules:
            continue
        if (node.lineno, names[0]) in seen:
            continue
        seen.add((node.lineno, names[0]))
        reached = bound[names[0]]
        for part in names[1:]:
            if not _is_module(f"{reached}.{part}"):
                break
            reached = f"{reached}.{part}"
        if reached not in modules:
            yield node.lineno, f"uses {reached}"


def _score_private(files: Dict[str, PythonFile], api_page: Path, score: Score) -> None:
    """K6: an import of desflow that the skill's API page does not name."""
    modules = _documented_modules(api_page)
    for name, python_file in files.items():
        for line, what in _private_imports(python_file.tree, modules):
            score.evidence["K6"].append(f"{name}:{line}: {what}")


def _score_hand_draws(env: Env, spec: dict, score: Score) -> None:
    """K13: a draw in the Low-level form that desflow does not seed, as `check_model.py`
    reports it, with the file run by its run child.

    Two runs at the file's own seed that differ show a draw with no seed. Each place where
    the file's own code seeds a random generator that no `Sampler` was given is a K13
    mistake, as `check_model.py`'s `_own_seeds` reports it.
    """
    first = run_seeded(env, spec, "low_level", None)
    if first is None:
        return
    if first.rc != 0:
        score.notes["K13"].append(
            "low_level.py did not run in the run child, so its draws were not checked"
        )
        return
    again = run_seeded(env, spec, "low_level", None, repeat=1)
    if (
        again is not None
        and again.rc == 0
        and stream_records(again.records) != stream_records(first.records)
    ):
        score.evidence["K13"].append(
            "low_level.py: two runs at its own seed differ, so it draws numbers with no seed"
        )
    rules = check_model()
    seeded = []
    for entry in first.seeded:
        seeded.append(rules.Seeded(entry.where, entry.call, entry.sampler))
    seen = rules.Seen(0, [], [], 1, [], seeded)
    form = rules.Form(Path("low_level.py"), rules.LOW_LEVEL, rules.Run([], seen))
    for problem in rules._own_seeds({"low_level": form}):
        score.evidence["K13"].append(f"{problem.where}: {problem.text}")


def _score_order(steady: Dict[str, Dict[str, str]], score: Score) -> None:
    """K12: a test, steady in each way of running, whose outcome differs between the ways.

    Args:
        steady: For each test that is not flaky, its outcome in each way of running.
        score: The score to add to.
    """
    for ident, by_way in sorted(steady.items()):
        if len(set(by_way.values())) > 1:
            shown = ", ".join(f"{way} {outcome}" for way, outcome in by_way.items())
            score.evidence["K12"].append(f"{ident}: {shown}")


def _score_changes(
    plain: Dict[str, str], changes: List[ChangeRun], score: Score
) -> None:
    """K9: an applied change under which no steady passing test of the agent's model fails.

    A change that some test fails is left to the reviewer, who reads which tests fail
    (`tests.json`) and judges whether one of them checks the behaviour the change is about.
    """
    passing = []
    for ident, outcome in plain.items():
        if outcome == "PASSED":
            passing.append(ident)
    for change in changes:
        if not change.applied:
            score.notes["K9"].append(f"{change.name}: {change.reason}")
            continue
        caught = []
        for ident in passing:
            # Why: a test flaky under the change neither catches it nor misses it for sure
            if outcome_of(change.outcomes, ident) not in ("PASSED", FLAKY):
                caught.append(ident)
        if caught:
            score.notes["K9"].append(
                f"{change.name}: failed by {', '.join(caught)} ({change.applied}); "
                "the reviewer judges these tests"
            )
            continue
        why = (
            "every test still passes"
            if passing
            else "no test passes on the agent's model"
        )
        score.evidence["K9"].append(f"{change.name}: {why} after {change.applied}")


def _score_tests(env: Env, spec: dict, mutations: dict, score: Score) -> dict:
    """BASE, K12 and K9, from the runs of the unchanged tests; returns `tests.json`.

    A test that fails in every run counts for BASE. A flaky test counts for neither BASE nor
    K12, is not one that passes for K9, and is written to `tests.json` for the reviewer.
    """
    if not (env.run_dir / "test_model.py").is_file():
        score.evidence["BASE"].append("test_model.py is missing")
        changes = run_changes(env, spec, mutations)
        _score_changes({}, changes, score)
        return tests_facts(Steady({}, {}, {}), changes)
    plain = run_steady(env, env.run_dir, trace=True)
    for ident, outcome in sorted(plain.outcomes.items()):
        if outcome == FLAKY:
            # Why: the outcomes of a flaky test differ between gradings; tests.json has them
            score.notes["BASE"].append(
                f"{ident} is flaky on the agent's own model (its runs are in tests.json)"
            )
        elif outcome != "PASSED":
            score.evidence["BASE"].append(f"{ident} fails on the agent's own model")
    steady = {}
    for ident, outcome in plain.outcomes.items():
        if outcome != FLAKY and ident != "<run>":
            steady[ident] = {way: found[0] for way, found in plain.runs[ident].items()}
    _score_order(steady, score)
    changes = run_changes(env, spec, mutations)
    if not changes:
        score.notes["K9"].append(
            "model.yaml does not run, so the changes were not tried"
        )
    _score_changes(plain.outcomes, changes, score)
    return tests_facts(plain, changes)


def _load(path: Path) -> dict:
    """Reads a task's YAML file."""
    return yaml.safe_load(path.read_text("utf-8"))


def _parse(argv: Optional[List[str]]) -> argparse.Namespace:
    """Reads the command line."""
    parser = argparse.ArgumentParser(description="Scores one evaluation run by script.")
    parser.add_argument("root", type=Path, help="a run folder that --keep-temp kept")
    parser.add_argument(
        "--task",
        required=True,
        help="the case folder name of the task, such as ds-a3; its tasks/ folder is ds_a3",
    )
    parser.add_argument(
        "--python-dir",
        type=Path,
        required=True,
        help="the prepared Python of the run's version (make_suite.py)",
    )
    parser.add_argument(
        "--check-python-dir",
        type=Path,
        default=None,
        help="the Python the run child of check_model.py runs with (K4, K1, K13); the "
        "prepared Python of the current tree, the same for every version; default --python-dir",
    )
    parser.add_argument(
        "--skill", type=Path, default=None, help="the skill copy the run read"
    )
    parser.add_argument(
        "--other-skill",
        type=Path,
        default=None,
        help="the skill copy of the other version, to hide the pages only one has",
    )
    parser.add_argument(
        "--api-page",
        type=Path,
        default=None,
        help="the API page K6 is judged against; default the --skill copy's references/api.md",
    )
    parser.add_argument("--tasks-dir", type=Path, default=HERE / "tasks")
    parser.add_argument("--cases-dir", type=Path, default=HERE / "cases")
    parser.add_argument(
        "--known-cases", type=Path, default=HERE.parents[2] / "tests" / "known_cases"
    )
    parser.add_argument(
        "--records-out",
        type=Path,
        default=None,
        help="a folder to write the facts the reviewer reads",
    )
    parser.add_argument(
        "--timeout", type=int, default=RUN_TIMEOUT, help="seconds a run may take"
    )
    parser.add_argument("--version", default="")
    parser.add_argument("--model", default="")
    parser.add_argument("--run", default="")
    args = parser.parse_args(argv)
    if args.api_page is None and args.skill is not None:
        args.api_page = args.skill / "references" / "api.md"
    if args.api_page is None:
        parser.error("K6 needs --skill, or --api-page for a run without the skill")
    return args


def _grade(args: argparse.Namespace, workspace: Path) -> Score:
    """Scores every kind of one run, and writes the reviewer's folder when asked."""
    # Why: a task is named by its case folder (`ds-a3`); its tasks/ and known-case folders
    # are named with `_`
    folder = args.task.replace("-", "_")
    spec = _load(args.tasks_dir / folder / "records.yaml")
    mutations = _load(args.tasks_dir / folder / "mutations.yaml")
    env = Env(
        workspace,
        args.python_dir.resolve(),
        args.timeout,
        (args.check_python_dir or args.python_dir).resolve(),
    )
    score = Score()
    try:
        files = _python_files(env)
        _score_records(env, spec, score)
        _score_forms_agree(env, spec, score)
        _score_varies(env, spec, score)
        _score_private(files, args.api_page, score)
        score_units(env, spec, files, score)
        _score_hand_draws(env, spec, score)
        tests = _score_tests(env, spec, mutations, score)
        if args.records_out is not None:
            sources = Sources(
                args.root,
                args.tasks_dir / folder,
                args.cases_dir / args.task,
                args.known_cases / folder / "original.py",
                args.skill,
                args.other_skill,
            )
            facts = {"tests.json": tests, "clocks.json": clock_asserts(spec, files)}
            hides = hidden_names(args.skill, args.other_skill)
            write_facts(env, sources, hides, facts, args.records_out)
    finally:
        env.close()
    return score


def main(argv: Optional[List[str]] = None) -> int:
    """Scores one kept run and prints one JSON line per kind.

    Args:
        argv: The command line without the program name; None reads `sys.argv`.

    Returns:
        The exit code: 0, or 2 when the run folder cannot be read.
    """
    args = _parse(argv)
    workspace = args.root / "sealed" / "home" / "cwd"
    try:
        readable = workspace.is_dir() and os.access(workspace, os.R_OK | os.X_OK)
    except OSError:
        readable = False
    if not readable:
        print(
            f"grade.py: cannot read {workspace}; open the kept folder first with "
            f"`chmod 700 {args.root} {args.root / 'sealed'}`",
            file=sys.stderr,
        )
        return 2
    score = _grade(args, workspace.resolve())
    for kind in KINDS:
        line: Dict[str, Any] = {
            "task": args.task,
            "kind": kind,
            "count": len(score.evidence[kind]),
            "evidence": score.evidence[kind][:20],
        }
        if score.notes[kind]:
            line["notes"] = score.notes[kind][:40]
        for field in ("version", "model", "run"):
            if getattr(args, field):
                line[field] = getattr(args, field)
        print(json.dumps(line))
    return 0


if __name__ == "__main__":
    sys.exit(main())
