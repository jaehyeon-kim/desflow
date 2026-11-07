"""Builds the suite that `claude plugin eval` runs for one version of the skill.

Usage:

    python make_suite.py --tree DIR --out DIR

`--tree` is a checkout of the repository at the version under test, with its `skills/desflow`
as `scripts/gen_skill.py` writes it (`test_committed_skill_is_current` keeps the two equal).
`--out` is a new folder outside every repository; give it a name that does not say the
version, such as `suite-a`, because the reviewer reads paths that hold it. The script writes:

- `OUT/wheel/`: the wheel of the tree (`uv build --wheel --offline`), base desflow only.
- `OUT/py/`: the prepared Python, a copy of uv's managed Python 3.12 with that wheel, pytest
  and pytest-xdist installed offline from uv's cache. A venv cannot be used, because its
  interpreter stays in the home folder, which the command's sandbox does not let the agent
  read. Each case's `setup.sh` copies this folder into the run's workspace.
- `OUT/desflow/`: a copy of the tree's `skills/desflow`, the command's target, with `evals/`
  holding a copy of each folder of `cases/` and `evals/python-path`, the path of `OUT/py`,
  which every `setup.sh` reads.

It prints the command's target, `OUT/desflow`. `grade.py` takes `OUT/py` as `--python-dir`
and `OUT/desflow` as `--skill`.
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

HERE = Path(__file__).resolve().parent
PYTHON = "3.12"
TEST_TOOLS = ["pytest", "pytest-xdist"]


def _run(cmd: List[str], cwd: Optional[Path] = None) -> str:
    """Runs a command and returns its standard output; a failure stops the script."""
    done = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if done.returncode != 0:
        raise SystemExit(
            f"make_suite.py: {' '.join(cmd)} failed:\n{done.stderr.strip()}"
        )
    return done.stdout


def _wheel(tree: Path, out: Path) -> Path:
    """Builds the tree's wheel into `out/wheel` and returns its path."""
    folder = out / "wheel"
    _run(["uv", "build", "--wheel", "--offline", "--out-dir", str(folder)], cwd=tree)
    wheels = sorted(folder.glob("*.whl"))
    if len(wheels) != 1:
        raise SystemExit(
            f"make_suite.py: wanted one wheel in {folder}, found {len(wheels)}"
        )
    return wheels[0]


def _prepared_python(wheel: Path, out: Path) -> Path:
    """Copies uv's managed Python into `out/py` and installs the wheel and the test tools."""
    found = _run(["uv", "python", "find", "--managed-python", PYTHON]).strip()
    # Why: the interpreter is `<root>/bin/python3.12`; the copy needs the whole root
    root = Path(found).resolve().parent.parent
    target = out / "py"
    shutil.copytree(root, target, symlinks=True)
    python = target / "bin" / f"python{PYTHON}"
    _run(
        [
            "uv",
            "pip",
            "install",
            "--offline",
            "--python",
            str(python),
            "--break-system-packages",
            str(wheel),
            *TEST_TOOLS,
        ]
    )
    return target


def _skill(tree: Path, out: Path, python_dir: Path) -> Path:
    """Copies the skill and the cases, and writes the path every `setup.sh` reads."""
    target = out / "desflow"
    # Why: a cached `.pyc` is not part of the skill, and a run would read a stale one
    shutil.copytree(
        tree / "skills" / "desflow",
        target,
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    evals = target / "evals"
    evals.mkdir()
    for case in sorted((HERE / "cases").iterdir()):
        if case.is_dir():
            shutil.copytree(case, evals / case.name)
    (evals / "python-path").write_text(f"{python_dir.resolve()}\n", "utf-8")
    return target


def main(argv: Optional[List[str]] = None) -> int:
    """Builds the suite of one version.

    Args:
        argv: The command line without the program name; None reads `sys.argv`.

    Returns:
        0; a failed step stops the script with its message.
    """
    parser = argparse.ArgumentParser(description="Builds one version's suite.")
    parser.add_argument("--tree", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    out = args.out.resolve()
    if out.exists():
        raise SystemExit(f"make_suite.py: {out} exists; give a new folder")
    out.mkdir(parents=True)
    wheel = _wheel(args.tree.resolve(), out)
    python_dir = _prepared_python(wheel, out)
    print(_skill(args.tree.resolve(), out, python_dir))
    return 0


if __name__ == "__main__":
    sys.exit(main())
