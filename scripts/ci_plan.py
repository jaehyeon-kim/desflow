"""Decides the test jobs of the workflows, for their `plan` jobs.

There are three sets (issue #90):

- `pr`, a pull request (`pull-request.yml`): the unit and conformance tests on Python
  3.11, the lowest version desflow supports.
- `main`, a push to `main` (`pipeline.yml`): the same on Python 3.12 to 3.15, which the
  pull request did not run. A pull request is up to date with `main` when it merges, so
  `main` holds the code it tested.
- `manual`, a run of `manual.yml` (the daily schedule, by hand, or the label
  `manual tests` on a pull request): every test, with the tests marked `every_model`
  and the known cases, on Python 3.12 on Ubuntu and Windows. `manual.yml` holds its
  other long jobs itself.

In `pr`, the docs tests run only when the files they read change, and the service
jobs only when the connectors, the examples or the dependencies change.

    python scripts/ci_plan.py manual < /dev/null
    git diff --name-only <base>...HEAD | python scripts/ci_plan.py pull_request

It writes `name=value` lines for `$GITHUB_OUTPUT`.

The parts of a test job are dealt by the times in `tests/durations.json`, measured on
the runners: the unit, conformance and benchtop tests without those marked
`every_model` take about 41 minutes of test time for one Python version, and about
21 minutes of test time run in about 11 minutes on a runner. So each version runs in
3 parts of about 8 minutes, so a push to `main` (4 versions) has 12 test jobs, within
the 20 jobs a run may have at once. With the tests marked `every_model` and the known cases, a
version takes about 145 minutes of test time, which `manual` runs in 7 parts.
"""

import fnmatch
import json
import sys
from typing import Dict, List

ALL = "tests/unit tests/conformance tests/benchtop tests/known_cases"
UNIT = "tests/unit tests/conformance tests/benchtop"
# The pull request tests the lowest version, and a push to main the others
PR_VERSION = "3.11"
MAIN_VERSIONS = ["3.12", "3.13", "3.14", "3.15"]
UNIT_PARTS = 3
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
    ".github/workflows/pull-request.yml",
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
    ".github/workflows/pull-request.yml",
    "scripts/ci_plan.py",
]

SYSTEMS = {
    "ubuntu-latest": "Ubuntu",
    "windows-latest": "Windows",
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
        return _leg("ubuntu-latest", PR_VERSION, False)
    if tier == "manual":
        # Python 3.12, the version in .python-version, with every test
        return [leg for os in SYSTEMS for leg in _leg(os, "3.12", True)]
    return [
        leg for python in MAIN_VERSIONS for leg in _leg("ubuntu-latest", python, False)
    ]


def _matches(path: str, patterns: List[str]) -> bool:
    return any(fnmatch.fnmatch(path, pattern) for pattern in patterns)


def plan(event: str, changed: List[str]) -> Dict[str, str]:
    """The jobs of a run. An empty `changed` means the changed files are not known,
    so every job that depends on them runs."""
    tier = {"pull_request": "pr", "push": "main"}.get(event, "manual")
    full = tier == "manual"
    known = bool(changed)
    pr = tier == "pr"
    code = full or not known or not all(_matches(p, DOCS_ONLY) for p in changed)
    docs = pr and (not known or any(_matches(p, DOCS) for p in changed))
    services = pr and (not known or any(_matches(p, SERVICES) for p in changed))
    return {
        "code": json.dumps(code),
        "docs": json.dumps(docs),
        "services": json.dumps(services),
        "unit": json.dumps({"include": unit_legs(tier)}),
    }


if __name__ == "__main__":
    lines = [line.strip() for line in sys.stdin if line.strip()]
    for key, value in plan(sys.argv[1], lines).items():
        print(f"{key}={value}")
