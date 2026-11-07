"""Runs the agent's tests: the outcome of each test, and which tests are steady or flaky.

Each run loads the plugin `grade_outcomes.py`, which writes the node id and outcome of every
test from pytest's own reports. The tests of a model run `RUNS` times in each of three ways:
plain (the first under the plugin `form_trace.py`, which records the forms of each test),
under `pytest -n 4`, and in reverse order. A test whose outcomes differ within one way is
`flaky`; any other test keeps the outcome of its plain runs.
"""

import json
from pathlib import Path
from typing import Dict, List, NamedTuple, Set

from grade_runs import Env

HERE = Path(__file__).resolve().parent
# Why: one run of a test that depends on timing can pass or fail, and a grade of the same
# folder must not change between gradings
RUNS = 3
FLAKY = "flaky"
# The ways a model's tests run, with the arguments and whether the order is reversed
WAYS = {"plain": False, "-n 4": False, "reversed": True}


class TestRun(NamedTuple):
    """The outcome of each test id of one pytest run, and the forms each ran when traced."""

    outcomes: Dict[str, str]
    forms: Dict[str, Set[str]]


class Steady(NamedTuple):
    """The tests of one model: each test's outcome or `flaky`, its runs, and its forms."""

    outcomes: Dict[str, str]
    runs: Dict[str, Dict[str, List[str]]]
    forms: Dict[str, Set[str]]


def run_pytest(
    env: Env, folder: Path, way: str = "plain", trace: bool = False
) -> TestRun:
    """Runs pytest once in `folder`, in one of `WAYS`; with `trace`, under `form_trace.py`."""
    outcomes_out = folder.parent / "outcomes.json"
    trace_out = folder.parent / "form-trace.json"
    plugins = ["-p", "grade_outcomes"] + (["-p", "form_trace"] if trace else [])
    parallel = ["-n", "4"] if way == "-n 4" else []
    extra_env = {
        "PYTHONPATH": str(HERE),
        "GRADE_OUTCOMES_OUT": str(outcomes_out),
        "GRADE_OUTCOMES_REVERSE": "1" if WAYS[way] else "0",
        "FORM_TRACE_OUT": str(trace_out),
    }
    outcomes_out.unlink(missing_ok=True)
    done = env.run(
        [env.python, "-m", "pytest", "-q", "--tb=no", "-p", "no:cacheprovider"]
        + plugins
        + parallel
        + ["test_model.py"],
        folder,
        timeout=300,
        extra_env=extra_env,
    )
    outcomes: Dict[str, str] = {}
    if outcomes_out.is_file():
        outcomes = json.loads(outcomes_out.read_text("utf-8"))
    if not outcomes and done.rc not in (0, 5):
        outcomes["<run>"] = "ERROR"
    forms: Dict[str, Set[str]] = {}
    if trace and trace_out.is_file():
        for ident, names in json.loads(trace_out.read_text("utf-8")).items():
            forms[ident] = set(names)
    return TestRun(outcomes, forms)


def outcome_of(outcomes: Dict[str, str], ident: str) -> str:
    """Returns a test's outcome; a run that collected nothing gives each test its error."""
    return outcomes.get(ident, outcomes.get("<run>", "NOT RUN"))


def run_steady(env: Env, folder: Path, trace: bool = False) -> Steady:
    """Runs the tests of the model in `folder` `RUNS` times in each way, each in a copy.

    Args:
        env: The grading environment.
        folder: The folder of the model, the agent's or a changed copy.
        trace: Whether the first plain run records the forms of each test.

    Returns:
        Each test's outcome, or `flaky` when its outcomes differ within one way.
    """
    results: Dict[str, List[Dict[str, str]]] = {}
    forms: Dict[str, Set[str]] = {}
    for way in WAYS:
        results[way] = []
        for number in range(RUNS):
            first = trace and way == "plain" and number == 0
            run = run_pytest(env, env.copy(folder), way, first)
            if first:
                forms = run.forms
            results[way].append(run.outcomes)
    idents = sorted(set().union(*results["plain"]))
    runs: Dict[str, Dict[str, List[str]]] = {}
    outcomes = {}
    for ident in idents:
        runs[ident] = {}
        for way, found in results.items():
            runs[ident][way] = [outcome_of(r, ident) for r in found]
        steady = all(len(set(seen)) == 1 for seen in runs[ident].values())
        outcomes[ident] = runs[ident]["plain"][0] if steady else FLAKY
    return Steady(outcomes, runs, forms)
