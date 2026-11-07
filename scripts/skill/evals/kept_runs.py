"""Lists the kept run folders of one `claude plugin eval` result file.

Usage:

    python kept_runs.py RESULT.json --version before [--first-run N]

`RESULT.json` is the file the command writes with `--json`. Each run of each case prints one
line: the task, the version, the run number and the kept folder, separated by tabs. The task
is the case's name, the folder name under `cases/` (`ds-a3`). A run of the arm `with` gets
`--version`; give `--version none` for a file whose arm `with` ran with no skill. A run of the
arm `without`, which the command adds under `--ablation with-without`, gets `none`, the
baseline. Run numbers count from `--first-run` (1 by default) in the order of the arm's runs,
so that a file holding runs 2 and 3 prints them as 2 and 3. The kept folder is the parent of
the run's `out/` folder, the folder `grade.py` reads. A run with an error, such as one whose
setup script failed or that stopped at a usage limit, prints `-` in place of the folder and
its error, even when it kept a folder, and the script exits 1: such a run is not graded.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import List, NamedTuple, Optional

ARMS = {"with": None, "without": "none"}


class KeptRun(NamedTuple):
    """One run of a result file."""

    task: str
    version: str
    run: int
    root: str
    error: str


def kept_runs(result: dict, version: str, first: int = 1) -> List[KeptRun]:
    """Returns every run of a result file, with its version and number.

    Args:
        result: The parsed result file.
        version: The version of the arm `with`.
        first: The number of each arm's first run.

    Returns:
        One entry per run, in the order of the cases and arms.
    """
    found = []
    for case in result.get("cases", []):
        for arm, label in ARMS.items():
            for number, run in enumerate(case.get("arms", {}).get(arm) or [], first):
                trace = run.get("tracePath") or ""
                error = str(run.get("error") or "")
                root = str(Path(trace).parent.parent) if trace and not error else "-"
                found.append(
                    KeptRun(
                        case["name"],
                        label or version,
                        number,
                        root,
                        error,
                    )
                )
    return found


def main(argv: Optional[List[str]] = None) -> int:
    """Prints one line per run.

    Args:
        argv: The command line without the program name; None reads `sys.argv`.

    Returns:
        0, or 1 when a run has an error or no kept folder.
    """
    parser = argparse.ArgumentParser(
        description="Lists the kept runs of a result file."
    )
    parser.add_argument("result", type=Path)
    parser.add_argument("--version", required=True)
    parser.add_argument("--first-run", type=int, default=1)
    args = parser.parse_args(argv)
    result = json.loads(args.result.read_text("utf-8"))
    runs = kept_runs(result, args.version, args.first_run)
    missing = False
    for run in runs:
        line = f"{run.task}\t{run.version}\t{run.run}\t{run.root}"
        if run.root == "-":
            missing = True
            line += f"\t{run.error}"
        print(line)
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
