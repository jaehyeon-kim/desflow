"""Decides which jobs a run of `pipeline.yml` runs, for its `plan` job.

A push to `main` or `release-1.0.0` runs the full set. A pull request runs a short set,
skips the code jobs when it changes only docs, and runs the integration and service
example jobs only when it changes what they cover (issue #90).

    python scripts/ci_plan.py push < /dev/null
    git diff --name-only <base>...HEAD | python scripts/ci_plan.py pull_request

It writes `name=value` lines for `$GITHUB_OUTPUT`.

The test legs are sized from the times in `tests/durations.json`, measured on the
runners (run 42609301404, Python 3.12, minutes of test time on Ubuntu): the tests
marked `every_model` take 71, `tests/known_cases` 33, and the rest of `tests/unit`,
`tests/conformance` and `tests/benchtop` 41. A share of about 21 minutes of test time
runs in about 11 minutes on the runner's 4 CPUs. So the tests marked `every_model` and
the known cases run on Python 3.12 only, the version in `.python-version`, on each
system, and the other versions run the rest.
"""

import fnmatch
import json
import sys
from typing import Dict, List

ALL = "tests/unit tests/conformance tests/benchtop tests/known_cases"
REST = "tests/unit tests/conformance tests/benchtop"

# Files whose change alone runs only the docs jobs
DOCS_ONLY = ["docs/*", "mkdocs.yml", "*.md"]

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


def _leg(
    os: str, python: str, label: str, paths: str, marks: str, count: int
) -> List[Dict]:
    system = "Ubuntu" if os.startswith("ubuntu") else "Windows"
    return [
        {
            "name": f"{system} {python} {label}"
            + (f" {share}/{count}" if count > 1 else ""),
            "os": os,
            "python": python,
            "paths": paths,
            "marks": marks,
            "shard": f"{share}/{count}" if count > 1 else "",
        }
        for share in range(1, count + 1)
    ]


def unit_legs(full: bool) -> List[Dict]:
    legs = _leg("ubuntu-latest", "3.12", "all", ALL, "not paced", 7)
    for python in ("3.11", "3.15"):
        legs += _leg(
            "ubuntu-latest", python, "rest", REST, "not paced and not every_model", 2
        )
    if full:
        for python in ("3.13", "3.14"):
            legs += _leg(
                "ubuntu-latest", python, "unit", "tests/unit", "not every_model", 1
            )
        legs += _leg("windows-latest", "3.12", "all", ALL, "not paced", 7)
    return legs


def paced_legs(full: bool) -> List[Dict]:
    # The paced cases time themselves below one tick of the Windows clock, which is
    # 15.6 ms before Python 3.13 and 1 microsecond from it (the Python 3.13
    # changelog, "time"), so the Windows runner runs them on 3.13.
    legs = [{"os": "ubuntu-latest", "python": "3.12"}]
    if full:
        legs.append({"os": "windows-latest", "python": "3.13"})
    return legs


def _matches(path: str, patterns: List[str]) -> bool:
    return any(fnmatch.fnmatch(path, pattern) for pattern in patterns)


def plan(event: str, changed: List[str]) -> Dict[str, str]:
    full = event != "pull_request"
    code = full or not changed or not all(_matches(p, DOCS_ONLY) for p in changed)
    services = full or any(_matches(path, SERVICES) for path in changed)
    return {
        "full": json.dumps(full),
        "code": json.dumps(code),
        "services": json.dumps(services),
        "unit": json.dumps({"include": unit_legs(full)}),
        "paced": json.dumps({"include": paced_legs(full)}),
        "simpy": json.dumps(["lowest", "newest"] if full else ["lowest"]),
    }


if __name__ == "__main__":
    lines = [line.strip() for line in sys.stdin if line.strip()]
    for key, value in plan(sys.argv[1], lines).items():
        print(f"{key}={value}")
