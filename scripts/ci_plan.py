"""Decides the test jobs of the workflows, for their `plan` jobs.

There are two sets (issue #90):

- `pr`, a pull request (`pull-request.yml`): the unit and conformance tests on Python
  3.11 to 3.15, every version desflow supports. A pull request is up to date with
  `main` when it merges, so `main` holds the code it tested, and a push to `main`
  (`pipeline.yml`) runs no test job.
- `manual`, a run of `manual.yml` (the daily schedule, by hand, or the label
  `manual tests` on a pull request): every test, with the tests marked `every_model`
  and the known cases, on Ubuntu for each Python version it is given (by default
  3.11, 3.12 and 3.15, the lowest, the one in .python-version and the newest) and on
  Windows for Python 3.12. `manual.yml` holds its other long jobs itself, and runs the
  integration tests and service examples (`services.yml`) whatever the change.

In `pr`, the docs tests run only when the files they read change, and the service
jobs only when what they cover changes: the connectors, the modules that build,
start and stop them, the examples, the gallery pages they read, their tests or the
dependencies.

    python scripts/ci_plan.py manual "3.11 3.12 3.15" < /dev/null
    git diff --name-only <base>...HEAD | python scripts/ci_plan.py pull_request

It writes `name=value` lines for `$GITHUB_OUTPUT`.

The parts of a test job are dealt by the times in `tests/durations.json`, measured on
the runners: the unit, conformance and benchtop tests without those marked
`every_model` take about 41 minutes of test time for one Python version, and about
21 minutes of test time run in about 11 minutes on a runner. So each version runs in
3 parts of about 8 minutes, and a pull request has 15 test jobs. With its other jobs it
starts 27, and the plan runs 20 at once. The 7 that wait start as the shortest jobs end,
at 4 to 7 minutes, and end before the longest docs part of about 14 minutes, so a pull
request takes about 17 minutes, as it did with Python 3.11 alone (simulated from the
job times of a run). With the tests marked `every_model` and the known cases, a version
takes about 145 minutes of test time, which `manual` runs in 7 parts.
"""

import fnmatch
import json
import sys
from typing import Dict, List, Optional

ALL = "tests/unit tests/conformance tests/benchtop tests/known_cases"
UNIT = "tests/unit tests/conformance tests/benchtop"
# The pull request tests every version desflow supports
PR_VERSIONS = ["3.11", "3.12", "3.13", "3.14", "3.15"]
# manual.yml runs every test on these on Ubuntu unless it is given others, and on
# Windows on 3.12
MANUAL_VERSIONS = ["3.11", "3.12", "3.15"]
UNIT_PARTS = 3
ALL_PARTS = 7

# Files no code job reads, whose change alone runs only the docs jobs. The pages and
# snippets under docs/ are not among them: the unit tests check each page's imports
# and run the snippet blueprints, and the regression captures them
DOCS_ONLY = ["mkdocs.yml", "docs/css/*", "docs/js/*", "docs/assets/*"]

# Files the docs tests read
DOCS = DOCS_ONLY + [
    "*.md",
    "docs/*",
    "src/*",
    "examples/*",
    "skills/*",
    "scripts/*",
    "hooks/*",
    "overrides/*",
    "tests/docs/*",
    "tests/conformance/*",
    "tests/known_cases/*",
    "tests/regression/*",
    "pyproject.toml",
    "uv.lock",
    ".github/workflows/pull-request.yml",
]

# Files the integration and service example jobs cover: the connectors, the modules
# that build them from a blueprint, the Builder or the CLI, the environment that
# starts, drains and stops them, and the gallery pages whose read-back commands the
# service examples run
SERVICES = [
    "src/desflow/connectors/*",
    "src/desflow/core/sinks.py",
    "src/desflow/core/environment.py",
    "src/desflow/blueprint/build.py",
    "src/desflow/builder/context.py",
    "src/desflow/cli/run.py",
    "src/desflow/__init__.py",
    "src/desflow/examples.py",
    "examples/*",
    "tests/integration/*",
    "tests/conftest.py",
    "docs/gallery/simulation-models/*",
    "pyproject.toml",
    "uv.lock",
    ".github/workflows/pull-request.yml",
    ".github/workflows/services.yml",
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


def unit_legs(tier: str, versions: Optional[List[str]] = None) -> List[Dict]:
    if tier == "pr":
        return [
            leg
            for python in PR_VERSIONS
            for leg in _leg("ubuntu-latest", python, False)
        ]
    # Every test, on Ubuntu for each version and on Windows for 3.12, the version in
    # .python-version
    ubuntu = [
        leg
        for python in versions or MANUAL_VERSIONS
        for leg in _leg("ubuntu-latest", python, True)
    ]
    return ubuntu + _leg("windows-latest", "3.12", True)


def _matches(path: str, patterns: List[str]) -> bool:
    return any(fnmatch.fnmatch(path, pattern) for pattern in patterns)


def _docs_only(path: str) -> bool:
    """Whether no code job reads the file: one of `DOCS_ONLY`, or a Markdown file at
    the top of the repository. `*` in fnmatch also matches `/`, so `*.md` alone would
    take in the pages under docs/."""
    return ("/" not in path and path.endswith(".md")) or _matches(path, DOCS_ONLY)


def plan(
    event: str, changed: List[str], versions: Optional[List[str]] = None
) -> Dict[str, str]:
    """The jobs of a run. An empty `changed` means the changed files are not known,
    so every job that depends on them runs. `versions` are the Python versions a
    `manual` run tests on Ubuntu, `MANUAL_VERSIONS` when not given."""
    tier = "pr" if event == "pull_request" else "manual"
    full = tier == "manual"
    known = bool(changed)
    pr = tier == "pr"
    code = full or not known or not all(_docs_only(p) for p in changed)
    docs = pr and (not known or any(_matches(p, DOCS) for p in changed))
    services = pr and (not known or any(_matches(p, SERVICES) for p in changed))
    return {
        "code": json.dumps(code),
        "docs": json.dumps(docs),
        "services": json.dumps(services),
        "unit": json.dumps({"include": unit_legs(tier, versions)}),
    }


if __name__ == "__main__":
    lines = [line.strip() for line in sys.stdin if line.strip()]
    chosen = sys.argv[2].split() if len(sys.argv) > 2 else None
    for key, value in plan(sys.argv[1], lines, chosen).items():
        print(f"{key}={value}")
