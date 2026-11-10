"""Records the time of each test from the JUnit files of a CI run.

The `unit-test` jobs of `manual.yml` upload a JUnit file each. Download the Ubuntu
ones of a run of `manual.yml` (the daily schedule, or a run by hand) and write the
times that `TEST_SHARD` deals the tests by:

    gh run download <run id> -p 'junit-Linux-*' -D /tmp/junit
    python scripts/test_durations.py /tmp/junit

Record them again when tests are added or become much slower, so the shares stay
even. A test that ran in more than one job keeps its longest time. Tests under half a
second, which hold about 2 per cent of the time, are left out and counted at their
mean time, under `__other__`, so the file stays small.
"""

import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "tests" / "durations.json"
# A test under this many seconds counts at the mean time of such tests
LISTED_FROM = 0.5
OTHER = "__other__"


def nodeid(classname: str, name: str) -> str:
    """`tests.unit.test_a.TestB` and `c` give `tests/unit/test_a.py::TestB::c`."""
    parts = classname.split(".")
    for cut in range(len(parts), 0, -1):
        path = Path(*parts[:cut]).with_suffix(".py")
        if (ROOT / path).is_file():
            return "::".join([path.as_posix(), *parts[cut:], name])
    raise ValueError(f"no test file for {classname}")


def main(folder: str) -> None:
    times: dict[str, float] = {}
    files = sorted(Path(folder).rglob("*.xml"))
    if not files:
        raise SystemExit(f"no JUnit file under {folder}")
    for file in files:
        for case in ET.parse(file).iter("testcase"):
            node = nodeid(case.get("classname", ""), case.get("name", ""))
            times[node] = max(
                times.get(node, 0.0), round(float(case.get("time") or 0), 3)
            )
    listed = {node: t for node, t in times.items() if t >= LISTED_FROM}
    rest = [t for t in times.values() if t < LISTED_FROM]
    listed[OTHER] = round(sum(rest) / len(rest), 3) if rest else 0.0
    OUT.write_text(json.dumps(dict(sorted(listed.items())), indent=0) + "\n")
    total = sum(times.values()) / 60
    print(f"{len(times)} tests from {len(files)} files, {total:.1f} minutes")


if __name__ == "__main__":
    main(sys.argv[1])
