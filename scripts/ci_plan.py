"""Decides which jobs a run of `pipeline.yml` runs, for its `plan` job.

There are three sets (issue #90):

- `pr`, a pull request: the unit and conformance tests on Python 3.11 and 3.15.
- `main`, a push to `main`: the same on every Python from 3.11 to 3.15.
- `full`, the daily schedule or a run started by hand: everything, adding the tests
  marked `every_model`, the known cases, Windows and macOS, the paced cases, the
  lowest Pydantic, the newest SimPy and the service jobs.

In `pr` and `main`, the docs tests run only when the files they read change, and the
service jobs only when the connectors, the examples or the dependencies change.

    python scripts/ci_plan.py schedule < /dev/null
    git diff --name-only <base>...HEAD | python scripts/ci_plan.py pull_request

It writes `name=value` lines for `$GITHUB_OUTPUT`.

The parts of a test job are dealt by the times in `tests/durations.json`, measured on
the runners: the unit, conformance and benchtop tests without those marked
`every_model` take about 41 minutes of test time for one Python version, and about
21 minutes of test time run in about 11 minutes on a runner. So each version runs in
4 parts of about 6 minutes. With the tests marked `every_model` and the known cases, a
version takes about 145 minutes of test time, which the full set runs in 7 parts.
"""

import fnmatch
import json
import sys
from typing import Dict, List

ALL = "tests/unit tests/conformance tests/benchtop tests/known_cases"
UNIT = "tests/unit tests/conformance tests/benchtop"
VERSIONS = ["3.11", "3.12", "3.13", "3.14", "3.15"]
UNIT_PARTS = 4
ALL_PARTS = 7

# Files whose change alone runs only the docs jobs
DOCS_ONLY = ["docs/*", "mkdocs.yml", "*.md"]

# Files the docs tests read, besides the docs themselves
DOCS = DOCS_ONLY + [
    "src/*",
    "examples/*",
    "skills/*",
    "scripts/*",
    "hooks/*",
    "overrides/*",
    "tests/docs/*",
    "pyproject.toml",
    "uv.lock",
    ".github/workflows/pipeline.yml",
]

# Files the integration and service example jobs cover
SERVICES = [
    "src/desflow/connectors/*",
    "src/desflow/core/sinks.py",
    "src/desflow/examples.py",
    "examples/*",
    "tests/integration/*",
    "pyproject.toml",
    "uv.lock",
    ".github/workflows/pipeline.yml",
    "scripts/ci_plan.py",
]

SYSTEMS = {
    "ubuntu-latest": "Ubuntu",
    "windows-latest": "Windows",
    "macos-latest": "macOS",
}


def _leg(os: str, python: str, everything: bool) -> List[Dict]:
    """The parts of one test job: the unit, conformance and benchtop tests, or with
    `everything` also the tests marked `every_model` and the known cases."""
    label = "All tests" if everything else "Unit and conformance tests"
    paths = ALL if everything else UNIT
    marks = "not paced" if everything else "not paced and not every_model"
    count = ALL_PARTS if everything else UNIT_PARTS
    return [
        {
            "name": f"{label}, Python {python}, {SYSTEMS[os]}, part {part} of {count}",
            "os": os,
            "python": python,
            "paths": paths,
            "marks": marks,
            "shard": f"{part}/{count}",
        }
        for part in range(1, count + 1)
    ]


def unit_legs(tier: str) -> List[Dict]:
    if tier == "pr":
        return _leg("ubuntu-latest", "3.11", False) + _leg(
            "ubuntu-latest", "3.15", False
        )
    legs: List[Dict] = []
    for python in VERSIONS:
        # The full set runs Python 3.12, the version in .python-version, with everything
        legs += _leg("ubuntu-latest", python, tier == "full" and python == "3.12")
    if tier == "full":
        legs += _leg("windows-latest", "3.12", True)
        legs += _leg("macos-latest", "3.12", True)
    return legs


def paced_legs(tier: str) -> List[Dict]:
    # The paced cases time themselves below one tick of the Windows clock, which is
    # 15.6 ms before Python 3.13 and 1 microsecond from it (the Python 3.13
    # changelog, "time"), so the Windows runner runs them on 3.13.
    if tier != "full":
        return []
    return [
        {"os": "ubuntu-latest", "python": "3.12"},
        {"os": "windows-latest", "python": "3.13"},
        {"os": "macos-latest", "python": "3.12"},
    ]


def _matches(path: str, patterns: List[str]) -> bool:
    return any(fnmatch.fnmatch(path, pattern) for pattern in patterns)


def plan(event: str, changed: List[str]) -> Dict[str, str]:
    """The jobs of a run. An empty `changed` means the changed files are not known,
    so every job that depends on them runs."""
    tier = {"pull_request": "pr", "push": "main"}.get(event, "full")
    full = tier == "full"
    known = bool(changed)
    code = full or not known or not all(_matches(p, DOCS_ONLY) for p in changed)
    docs = full or not known or any(_matches(p, DOCS) for p in changed)
    services = full or not known or any(_matches(p, SERVICES) for p in changed)
    paced = paced_legs(tier)
    return {
        "full": json.dumps(full),
        "code": json.dumps(code),
        "docs": json.dumps(docs),
        "services": json.dumps(services),
        "unit": json.dumps({"include": unit_legs(tier)}),
        "paced": json.dumps({"include": paced or [{"os": "", "python": ""}]}),
        "has_paced": json.dumps(bool(paced)),
        "simpy": json.dumps(["lowest", "newest"] if full else ["lowest"]),
    }


if __name__ == "__main__":
    lines = [line.strip() for line in sys.stdin if line.strip()]
    for key, value in plan(sys.argv[1], lines).items():
        print(f"{key}={value}")
