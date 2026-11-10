"""Shows that the evaluation's scripts work, on inputs whose right results are known.

Usage: python selftest.py --python-dir DIR [--grade PATH] [--summarise PATH]
    [--kept-runs PATH] [--case NAME ...]

`--python-dir` is a prepared Python (`make_suite.py` writes one as `OUT/py`), or any Python
folder with desflow, pytest and pytest-xdist; `grade.py` runs the forms and the tests with it.

Each grading case is a folder with a `case.json`: `clean/` (no mistake of any kind), `planted/`
(one planted mistake of each kind the script scores) and one folder under `cases/` for each
check, or each fault a review found, which the grader must not make again. The case's files
are laid out as `claude plugin eval --keep-temp` keeps a run: the workspace in
`sealed/home/cwd` and the case's `trace.jsonl`, or an empty one, in `out/`. `case.json` gives
the task, `tasks` (`main` for a task of `../tasks`, otherwise one of `tasks/` here), the
`counts` the grader must give, the texts that its `evidence` and `notes` of a kind must contain
(`evidence`, `notes`) or must not contain (`notes_absent`), the forms `tests.json` must give a
test id (`forms`), and the texts a file of the reviewer's folder must contain (`facts`, by its
path in the folder) or must not contain (`facts_absent`; an empty list: the file must not exist). With `hidden: true`, `check_hidden.py` must find no name of a version in that folder. With `skill: true`, the case is graded with `--skill` and `--other-skill`,
two skill copies made for it, of which only the first has `references/testing.md`; `<skill>`
in the case's files stands for the first copy's path. K6 is judged against `api/<api>.md`, the
API page of the skill before the changes (`before`) or after them (`after`, the default), as
`api` in `case.json` gives. The Python files are kept as `.py.txt`,
so that the repository's linters skip them. The case `unreadable` checks that `grade.py` exits
2 on a folder whose workspace it cannot read.

`summarise/` holds lines of runs with known answers: for each name, `<name>.grades.jsonl`,
`<name>.reviews.jsonl` and `<name>.want.json`, the exit code and the texts the output must
contain; its `cases/` holds the two task names those lines use. `kept_runs/` holds, for each
name, `<name>.result.json`, a result file of `claude plugin eval` written for the test, and
`<name>.want.json`, the arguments, the exit code and the lines `kept_runs.py` must print.
`--grade`, `--summarise` and `--kept-runs` run other copies of the scripts, such as a copy
with one check turned off, to show a case failing there.
"""

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import check_hidden  # noqa: E402

_RUN_FILES = ["model.yaml", "builder.py", "low_level.py", "test_model.py", "reply.md"]


class _Result(NamedTuple):
    """The result of one case: its name and what differs from what it wants."""

    name: str
    problems: List[str]


class _Skills(NamedTuple):
    """The two skill copies a case is graded with."""

    skill: Path
    other: Path


def _cases() -> Dict[str, Path]:
    """Returns every grading case folder by its name."""
    folders = [HERE / "clean", HERE / "planted", *sorted((HERE / "cases").iterdir())]
    cases = {}
    for folder in folders:
        if (folder / "case.json").is_file():
            cases[folder.name] = folder
    return cases


def _skills(parent: Path, api: str) -> _Skills:
    """Makes two skill copies: both have `api.md`, only the first has `testing.md`.

    The first copy's `api.md` is the page `api/<api>.md`, the API page of one version of the
    skill, which K6 is judged against.
    """
    skill, other = parent / "skill", parent / "other"
    for folder, pages in ((skill, ["api.md", "testing.md"]), (other, ["api.md"])):
        (folder / "references").mkdir(parents=True)
        (folder / "SKILL.md").write_text("See references/api.md.\n", "utf-8")
        for page in pages:
            (folder / "references" / page).write_text(f"# {page}\n", "utf-8")
    shutil.copy(HERE / "api" / f"{api}.md", skill / "references" / "api.md")
    return _Skills(skill, other)


def _copy(folder: Path, root: Path, skill: Path) -> None:
    """Lays a case out as a kept run folder, with `<skill>` replaced by the skill's path."""
    workspace = root / "sealed" / "home" / "cwd"
    workspace.mkdir(parents=True)
    (root / "out").mkdir()
    for name in _RUN_FILES:
        for source in (folder / name, folder / f"{name}.txt"):
            if source.is_file():
                text = source.read_text("utf-8").replace("<skill>", str(skill))
                (workspace / name).write_text(text, "utf-8")
    trace = folder / "trace.jsonl"
    text = trace.read_text("utf-8") if trace.is_file() else ""
    (root / "out" / "trace.jsonl").write_text(
        text.replace("<skill>", str(skill)), "utf-8"
    )


def _line_problems(case: dict, lines: Dict[str, dict]) -> List[str]:
    """Lists each way the grader's lines differ from what a case wants."""
    found = []
    for kind, count in case["counts"].items():
        got = lines.get(kind, {}).get("count")
        if got != count:
            found.append(f"{kind} count {got}, wanted {count}")
    for field in ("evidence", "notes"):
        for kind, texts in case.get(field, {}).items():
            have = " | ".join(lines.get(kind, {}).get(field, []))
            for text in texts:
                if text not in have:
                    found.append(f"{kind} {field} lacks '{text}'")
    for kind, texts in case.get("notes_absent", {}).items():
        have = " | ".join(lines.get(kind, {}).get("notes", []))
        for text in texts:
            if text in have:
                found.append(f"{kind} notes hold '{text}'")
    return found


def _fact_problems(case: dict, out: Path) -> List[str]:
    """Lists each way the reviewer's folder differs from what a case wants."""
    found = []
    tests_file = out / "tests.json"
    tests = (
        json.loads(tests_file.read_text("utf-8"))["tests"]
        if tests_file.is_file()
        else {}
    )
    for ident, forms in case.get("forms", {}).items():
        got = tests.get(ident, {}).get("forms")
        if got != forms:
            found.append(f"forms of {ident}: {got}, wanted {forms}")
    for name, texts in case.get("facts", {}).items():
        path = out / name
        have = path.read_text("utf-8") if path.is_file() else ""
        for text in texts:
            if text not in have:
                found.append(f"{name} lacks '{text}'")
    for name, texts in case.get("facts_absent", {}).items():
        path = out / name
        if not texts and path.exists():
            found.append(f"{name} exists")
        have = path.read_text("utf-8") if path.is_file() else ""
        for text in texts:
            if text in have:
                found.append(f"{name} holds '{text}'")
    if case.get("hidden"):
        found += [f"names a version: {leak}" for leak in check_hidden.leaks(out)]
    return found


def _grade(name: str, folder: Path, args: argparse.Namespace) -> _Result:
    """Grades one case; returns its name and its problems."""
    case = json.loads((folder / "case.json").read_text("utf-8"))
    parent = Path(tempfile.mkdtemp(prefix="selftest-"))
    skills = _skills(parent, case.get("api", "after"))
    root, out = parent / "e-case", parent / "review"
    _copy(folder, root, skills.skill)
    tasks = HERE.parent / "tasks" if case.get("tasks") == "main" else HERE / "tasks"
    cmd = [
        str(args.python_dir / "bin" / "python"),
        str(args.grade),
        str(root),
        "--task",
        case["task"],
        "--tasks-dir",
        str(tasks),
        "--python-dir",
        str(args.python_dir),
        "--records-out",
        str(out),
        "--api-page",
        str(skills.skill / "references" / "api.md"),
    ]
    if case.get("skill"):
        cmd += ["--skill", str(skills.skill), "--other-skill", str(skills.other)]
    done = subprocess.run(cmd, capture_output=True, text=True)
    lines = {}
    for line in done.stdout.splitlines():
        row = json.loads(line)
        lines[row["kind"]] = row
    problems = _line_problems(case, lines) + _fact_problems(case, out)
    shutil.rmtree(parent, ignore_errors=True)
    if not lines:
        return _Result(name, [f"no output: {done.stderr.strip().splitlines()[-1:]}"])
    return _Result(name, problems)


def _unreadable(args: argparse.Namespace) -> List[str]:
    """Runs `grade.py` on a kept folder with no workspace; it must exit 2."""
    root = Path(tempfile.mkdtemp(prefix="selftest-unreadable-"))
    done = subprocess.run(
        [
            str(args.python_dir / "bin" / "python"),
            str(args.grade),
            str(root),
            "--task",
            "p33",
            "--python-dir",
            str(args.python_dir),
            "--api-page",
            str(HERE / "api" / "after.md"),
        ],
        capture_output=True,
        text=True,
    )
    shutil.rmtree(root, ignore_errors=True)
    if done.returncode != 2 or "chmod 700" not in done.stderr:
        return [
            f"exit {done.returncode}, wanted 2 with the chmod line: {done.stderr[-200:]}"
        ]
    return []


def _summarise(args: argparse.Namespace) -> List[str]:
    """Runs `summarise.py` on the lines of `summarise/`; returns its problems."""
    found = []
    for want in sorted((HERE / "summarise").glob("*.want.json")):
        spec = json.loads(want.read_text("utf-8"))
        stem = want.name[: -len(".want.json")]
        done = subprocess.run(
            [
                sys.executable,
                str(args.summarise),
                "--cases-dir",
                str(HERE / "summarise" / "cases"),
                "--grades",
                str(want.with_name(f"{stem}.grades.jsonl")),
                "--reviews",
                str(want.with_name(f"{stem}.reviews.jsonl")),
            ],
            capture_output=True,
            text=True,
        )
        if done.returncode != spec["rc"]:
            found.append(f"{stem}: exit {done.returncode}, wanted {spec['rc']}")
        output = done.stdout + done.stderr
        for text in spec["contains"]:
            if text not in output:
                found.append(f"{stem}: lacks '{text}'")
    return found


def _kept_runs(args: argparse.Namespace) -> List[str]:
    """Runs `kept_runs.py` on each result file of `kept_runs/`; returns its problems."""
    found = []
    for want in sorted((HERE / "kept_runs").glob("*.want.json")):
        spec = json.loads(want.read_text("utf-8"))
        stem = want.name[: -len(".want.json")]
        result = want.with_name(f"{stem}.result.json")
        done = subprocess.run(
            [sys.executable, str(args.kept_runs), str(result)] + spec["args"],
            capture_output=True,
            text=True,
        )
        if done.returncode != spec["rc"]:
            found.append(f"{stem}: exit {done.returncode}, wanted {spec['rc']}")
        if done.stdout != spec["stdout"]:
            found.append(f"{stem}: printed {done.stdout!r}"[:400])
    return found


def main(argv: Optional[List[str]] = None) -> int:
    """Grades every case and checks the other scripts; prints one line per case.

    Args:
        argv: The command line without the program name; None reads `sys.argv`.

    Returns:
        0 when every case gives what it wants, else 1.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--python-dir", type=Path, required=True)
    parser.add_argument("--grade", type=Path, default=HERE.parent / "grade.py")
    parser.add_argument("--summarise", type=Path, default=HERE.parent / "summarise.py")
    parser.add_argument("--kept-runs", type=Path, default=HERE.parent / "kept_runs.py")
    parser.add_argument("--case", action="append", help="run only this case")
    args = parser.parse_args(argv)
    args.python_dir = args.python_dir.resolve()
    cases = {}
    for name, folder in _cases().items():
        if not args.case or name in args.case:
            cases[name] = folder
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda name: _grade(name, cases[name], args), cases))
    others = {
        "unreadable": _unreadable,
        "summarise": _summarise,
        "kept_runs": _kept_runs,
    }
    for name, check in others.items():
        if not args.case or name in args.case:
            results.append(_Result(name, check(args)))
    failed = False
    for result in results:
        failed |= bool(result.problems)
        verdict = (
            "DIFFERENT: " + "; ".join(result.problems) if result.problems else "ok"
        )
        print(f"{result.name}: {verdict}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
