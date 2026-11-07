"""A pytest plugin that writes the outcome of each test, as pytest's own reports give it.

`grade.py` loads it into every test run with `-p grade_outcomes`. With `GRADE_OUTCOMES_OUT`
set, it writes a JSON object from each test's node id to its outcome when the session ends:
`PASSED`, `FAILED`, `ERROR` (its setup or teardown failed), `SKIPPED`, `XFAIL` or `XPASS`.
A module or folder that fails to be collected gives `<run>` the outcome `ERROR`. With
`GRADE_OUTCOMES_REVERSE=1`, the collected tests run in reverse order. Under `pytest -n 4`,
the workers' reports reach the main process, which writes the file.
"""

import json
import os
from pathlib import Path
from typing import Dict, List

import pytest

_OUTCOMES: Dict[str, str] = {}


def pytest_collection_modifyitems(
    session: pytest.Session, config: pytest.Config, items: List[pytest.Item]
) -> None:
    """Reverses the order of the tests when asked.

    Args:
        session: The pytest session.
        config: The pytest configuration.
        items: The collected tests, changed in place.
    """
    if os.environ.get("GRADE_OUTCOMES_REVERSE") == "1":
        items.reverse()


def pytest_collectreport(report: pytest.CollectReport) -> None:
    """Records a collection that failed.

    Args:
        report: The report of one collector.
    """
    if report.failed:
        _OUTCOMES["<run>"] = "ERROR"


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    """Records the outcome of one phase of a test; a failed setup or teardown is an error.

    Args:
        report: The report of one phase.
    """
    ident = report.nodeid
    if report.when == "call":
        if hasattr(report, "wasxfail"):
            outcome = "XPASS" if report.passed else "XFAIL"
        else:
            outcome = report.outcome.upper()
        if _OUTCOMES.get(ident) != "ERROR":
            _OUTCOMES[ident] = outcome
    elif report.failed:
        _OUTCOMES[ident] = "ERROR"
    elif report.skipped and ident not in _OUTCOMES:
        _OUTCOMES[ident] = "XFAIL" if hasattr(report, "wasxfail") else "SKIPPED"


def pytest_sessionfinish(session: pytest.Session) -> None:
    """Writes the outcomes, from the main process only.

    Args:
        session: The pytest session.
    """
    target = os.environ.get("GRADE_OUTCOMES_OUT")
    if target and not hasattr(session.config, "workerinput"):
        Path(target).write_text(json.dumps(_OUTCOMES, sort_keys=True), "utf-8")
