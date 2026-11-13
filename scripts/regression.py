"""Regression capture and compare against a baseline release.

Layer 3 of the 0.23.0 regression guard (see release/0.23.0/progress.md). It runs every
example and every conformance blueprint unpaced, captures every event, label and
telemetry record, and compares a baseline capture with the working tree's, field by
field. Each side runs its own files, so a changed example, runner or blueprint never
runs on the baseline:

    uv run --all-extras python scripts/regression.py capture --root . --out new.json
    cd /tmp && uv run --no-project --python 3.12 --with "desflow[all]==0.22.0" \\
        --with pytest python <repository>/scripts/regression.py capture \\
        --root <v0.22.0 checkout> --scenarios <repository>/tests/regression/scenarios \\
        --left-out <repository>/tests/regression/left_out.yaml --out /tmp/old.json
    uv run --all-extras python scripts/regression.py compare /tmp/old.json new.json \\
        --baseline 0.22.0

`capture --shard 2/6` runs the second of six parts, and `merge --out new.json
new-*.json` joins the parts of one side before `compare`. A run goes to the same part
on each side, so the CI pipeline captures the parts in jobs at the same time.

Run the old side from a directory outside the repository, such as `/tmp`, and name
the working tree's script by its absolute path. From inside the repository,
`uv run --no-project` imports the working tree's desflow, not the released wheel
(`desflow.__file__` is then `src/desflow/__init__.py`; from `/tmp` it is the PyPI
package).

`--root` is put first on `sys.path`, with its `tests/docs`, so the capture imports
that checkout's twin-test runners and its `tests.conformance`; the desflow it runs is
the one installed. Records are compared without `timestamp` and `system.` paths, as
the twin test compares them.

Every other YAML blueprint under `examples/`, `tests/` and `docs/` is captured too
(`_all_blueprints`), run unpaced into a capture in place of its own sinks, and a
refusal is kept as its error. Each file runs with the `sys.modules` and `sys.path` it
started with, so a `!python` module that one file imports is not found by the next. A
file is left out by the list in `tests/regression/left_out.yaml` of the checkout, with
its reason, when it is not a blueprint, has no end, needs a running service, gives only
an import error because its `!python` module is neither beside it nor in a folder it
names, or gives a different capture on two runs of one commit. `--left-out` names another list, and the baseline
side passes the working tree's, as it does the scenarios. A file that does not end
within 180 s stops the capture and is named, so give it `run.until` or list it.

The regression scenarios (`tests/regression/scenarios`, listed in `cases.py` beside
it) are not in a v0.22.0 checkout: `--scenarios` names the folder to run, by default
`<root>/tests/regression/scenarios`, and the baseline side passes the working tree's
folder, so both sides run the same files with their own desflow. Each scenario is run
in YAML, the builder and the low-level API, and the capture stops when the three give
different records.

`compare` without `--baseline` fails on any difference. With `--baseline <version>` it
reads `tests/regression/expected.yaml` (or `--expected`): a run only in the new
capture is reported as added; a run only in the old capture, or with different
records, fails unless an entry lists its key with a `since` above the baseline; an
entry that matches no difference fails too, so the file keeps no stale entries.
`--set-aside-absent` first sets aside the entries whose run is in neither capture, for
the comparison of the files of one release, which holds no run added later.
"""

import argparse
import contextlib
import importlib.util
import json
import re
import signal
import sys
import zlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import yaml

EXPECTED = (
    Path(__file__).resolve().parents[1] / "tests" / "regression" / "expected.yaml"
)
# Seconds one file of `_all_blueprints` may run. The limit stops a file with no end;
# the slowest file takes 30 s on a laptop and over 60 s on a CI runner.
RUN_LIMIT = 180
# The part of the runs this capture takes, as (part, count), set by `--shard`
SHARD = (1, 1)


def _mine(key: str) -> bool:
    """Whether this capture's part runs the run or group of runs named `key`.

    A key goes to the same part on every machine, so the parts of the last release
    and of this commit hold the same runs.
    """
    part, count = SHARD
    return zlib.crc32(key.encode()) % count == part - 1


def _load(root: Path):
    """Imports the twin-test machinery and the conformance harness of `root`."""
    sys.path.insert(0, str(root / "tests" / "docs"))
    sys.path.insert(0, str(root))
    import test_example_twins as twins  # from <root>/tests/docs
    from tests.conformance import harness

    return twins, harness


def _examples(twins) -> Dict[str, Any]:
    """Every example, in each API, captured through the twin-test runners."""
    captured: Dict[str, Any] = {}
    for name, yaml_file, extra, until, processes in twins.EXAMPLE_CASES:
        if extra and importlib.util.find_spec(extra) is None:
            continue
        if not _mine(f"example:{name}"):
            continue
        captured[f"example:{name}:builder"] = twins._builder_records(name, until)
        low = twins._load(twins.EXAMPLES / "low-level" / f"{name}_example.py")
        captured[f"example:{name}:low_level"] = twins._run_low_level(
            low, until, lambda env, s, p=processes, m=low: p(m, env, s)
        )
        yaml_app = twins._from_yaml(twins.EXAMPLES / "yaml" / yaml_file)
        captured[f"example:{name}:yaml"] = twins._run_builder(
            yaml_app, until, **twins.RUN.get(name, {})
        )
    return captured


def _blueprints(root: Path, twins, harness) -> Dict[str, Any]:
    """Every conformance blueprint, run bare; one that raises is kept as its error, so
    a refusal fixture is compared by its exception rather than its records."""
    captured: Dict[str, Any] = {}
    folder = root / "tests" / "conformance" / "blueprints"
    for path in sorted(folder.glob("*.yaml")):
        if not _mine(f"blueprint:{path.name}"):
            continue
        try:
            records, _ = harness.run_yaml(path.name)
            captured[f"blueprint:{path.name}"] = twins._records(records)
        except Exception as exc:  # the exception is the captured value
            text = f"{type(exc).__name__}: {exc}"
            # The checkout's own place is not part of the message
            captured[f"blueprint:{path.name}"] = {
                "error": text.replace(root.as_posix(), "<root>")
            }
    return captured


def _left_out(path: Path) -> Dict[str, str]:
    """The files `_all_blueprints` leaves out, each with its reason.

    Args:
        path: The YAML file that maps a path, relative to the checkout and written
            with forward slashes, to the reason the file is left out.

    Returns:
        Dict[str, str]: The reason of each file.

    Raises:
        SystemExit: When the file is missing, or an entry has no reason.
    """
    if not path.is_file():
        raise SystemExit(f"no list of left-out files at {path}; pass --left-out")
    entries = yaml.safe_load(path.read_text()) or {}
    empty = sorted(name for name, reason in entries.items() if not str(reason).strip())
    if empty:
        raise SystemExit(f"{path}: no reason for {empty}")
    return {str(name): str(reason) for name, reason in entries.items()}


def _run_by_another_capture(name: str, path: Path, taken: set) -> bool:
    """Whether the examples, the conformance blueprints or the scenarios run this file.

    Args:
        name: The path relative to the checkout, with forward slashes.
        path: The file.
        taken: The resolved paths of the YAML files of `EXAMPLE_CASES`.

    Returns:
        bool: True when another capture runs the file.
    """
    folders = ("tests/conformance/blueprints/", "tests/regression/scenarios/")
    return name.startswith(folders) or path.resolve() in taken


def _other_blueprints(root: Path, twins, left_out: Dict[str, str]) -> List[Path]:
    """Every YAML file under `examples/`, `tests/` and `docs/` that no other capture
    runs and that is not left out, in path order.

    The other captures run the YAML files of `EXAMPLE_CASES`, the files of
    `tests/conformance/blueprints` and the scenarios.
    """
    taken = {
        (twins.EXAMPLES / "yaml" / case[1]).resolve() for case in twins.EXAMPLE_CASES
    }
    found: List[Path] = []
    for folder in ("examples", "tests", "docs"):
        found.extend(
            path
            for path in (root / folder).rglob("*")
            if path.suffix.lower() in (".yaml", ".yml") and path.is_file()
        )
    other: List[Path] = []
    for path in sorted(found):
        name = path.relative_to(root).as_posix()
        if name not in left_out and not _run_by_another_capture(name, path, taken):
            other.append(path)
    return other


@contextlib.contextmanager
def _time_limit(name: str, seconds: int):
    """Stops the capture, naming the file, when its run lasts more than `seconds`.

    The limit needs `SIGALRM`, so it does nothing where the platform has none. No value
    is recorded for a file that hits it, because a time limit would make the two sides
    differ by machine.
    """
    if not hasattr(signal, "SIGALRM"):
        yield
        return

    def expired(signum, frame):
        raise SystemExit(
            f"{name} did not end within {seconds} s; give it run.until or add it "
            "to tests/regression/left_out.yaml"
        )

    previous = signal.signal(signal.SIGALRM, expired)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


@contextlib.contextmanager
def _restored_imports():
    """Puts `sys.modules` and `sys.path` back as they were when the block ends."""
    modules = dict(sys.modules)
    path = list(sys.path)
    try:
        yield
    finally:
        sys.modules.clear()
        sys.modules.update(modules)
        sys.path[:] = path


def _all_blueprints(
    root: Path, twins, left_out: Dict[str, str], seconds: int = RUN_LIMIT
) -> Dict[str, Any]:
    """Every other YAML blueprint of a checkout, run unpaced into a capture in place of
    its own sinks, from the checkout's folder. A file that raises is kept as its error,
    as `_blueprints` keeps one, with the checkout's path written as `<root>`. A file
    that does not end within `seconds` stops the capture. Each file runs with the
    `sys.modules` and `sys.path` it started with, restored after it, so a `!python`
    module that one file imports is not found by the next, and a file's capture depends
    on that file alone."""
    captured: Dict[str, Any] = {}
    for path in _other_blueprints(root, twins, left_out):
        name = path.relative_to(root).as_posix()
        if not _mine(f"file:{name}"):
            continue
        try:
            with (
                _restored_imports(),
                contextlib.chdir(root),
                _time_limit(name, seconds),
            ):
                # a file reads its data beside the checkout
                captured[f"file:{name}"] = twins._run_builder(
                    twins._from_yaml(path), None
                )
        except Exception as exc:  # the exception is the captured value
            text = f"{type(exc).__name__}: {exc}"
            # The checkout's own place is not part of the message
            captured[f"file:{name}"] = {
                "error": text.replace(root.as_posix(), "<root>")
            }
    return captured


def _scenario_cases(folder: Path):
    """The `cases.py` that sits beside a scenarios folder, loaded by its path."""
    path = folder.parent / "cases.py"
    spec = importlib.util.spec_from_file_location("regression_cases", path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _scenario_files(folder: Path, names: List[str]) -> List[str]:
    """What is wrong with the scenarios folder: a file missing for a name in the table,
    or a file whose name is not in it."""
    problems = []
    for api, suffix, pattern in (
        ("yaml", ".yaml", "*.yaml"),
        ("builder", "_example.py", "*_example.py"),
        ("low-level", "_example.py", "*_example.py"),
    ):
        found = {p.name.removesuffix(suffix) for p in (folder / api).glob(pattern)}
        problems += [
            f"{api}/{n}{suffix} is not in the folder" for n in names if n not in found
        ]
        problems += [
            f"{api}/{n}{suffix} is not in SCENARIO_CASES" for n in found - set(names)
        ]
    return problems


def _scenarios(twins, folder: Path) -> Dict[str, Any]:
    """Every scenario, in each API, through the twin-test runners, loading its files
    from `folder`. The three forms of a scenario must give the same records."""
    cases = _scenario_cases(folder)
    problems = _scenario_files(folder, [case[0] for case in cases.SCENARIO_CASES])
    if problems:
        raise SystemExit("scenarios folder: " + "; ".join(problems))
    captured: Dict[str, Any] = {}
    for name, yaml_file, _extra, until, processes in cases.SCENARIO_CASES:
        if not _mine(f"scenario:{name}"):
            continue
        run = cases.SCENARIO_RUN.get(name, {})
        builder = twins._load(folder / "builder" / f"{name}_example.py")
        app = builder.app
        low = twins._load(folder / "low-level" / f"{name}_example.py")
        forms = {
            "builder": twins._run_builder(app, until, **run),
            "low_level": twins._run_low_level(
                low, until, lambda env, s, p=processes, m=low: p(m, env, s)
            ),
            "yaml": twins._run_builder(
                twins._from_yaml(folder / "yaml" / yaml_file), until, **run
            ),
        }
        for api, records in forms.items():
            if not records:
                raise SystemExit(f"scenario {name}: the {api} form gives no records")
            if records != forms["builder"]:
                raise SystemExit(
                    f"scenario {name}: the {api} form gives different records "
                    "from the builder form"
                )
            captured[f"scenario:{name}:{api}"] = records
    return captured


def capture(
    root: str,
    out: str,
    scenarios: Optional[str] = None,
    left_out: Optional[str] = None,
) -> None:
    """Runs every example, blueprint and scenario of a checkout and writes their records.

    Args:
        root: The checkout to run.
        out: The JSON file to write.
        scenarios: The scenarios folder; `<root>/tests/regression/scenarios` when None.
        left_out: The list of files to leave out; `<root>/tests/regression/left_out.yaml`
            when None.

    Raises:
        SystemExit: When the scenarios folder or the list of left-out files does not
            exist, or an entry of the list has no reason.
    """
    root_path = Path(root).resolve()
    twins, harness = _load(root_path)
    folder = Path(scenarios or root_path / "tests" / "regression" / "scenarios")
    if not folder.is_dir():
        raise SystemExit(f"no scenarios folder at {folder}; pass --scenarios")
    skipped = _left_out(
        Path(left_out or root_path / "tests" / "regression" / "left_out.yaml")
    )
    with patch("pyiceberg.catalog.rest.RestCatalog", MagicMock()):
        captured = {**_examples(twins), **_blueprints(root_path, twins, harness)}
        captured.update(_all_blueprints(root_path, twins, skipped))
        captured.update(_scenarios(twins, folder.resolve()))
    Path(out).write_text(json.dumps(captured, default=str, sort_keys=True))
    print(f"captured {len(captured)} runs from {root_path} to {out}")


def merge(parts: List[str], out: str) -> None:
    """Joins the capture files of the parts of one side into one file.

    Args:
        parts: The capture files of the parts.
        out: The JSON file to write.

    Raises:
        SystemExit: When two parts hold the same run.
    """
    joined: Dict[str, Any] = {}
    for part in parts:
        captured = json.loads(Path(part).read_text())
        both = sorted(set(joined) & set(captured))
        if both:
            raise SystemExit(f"{part} repeats runs of another part: {both[:5]}")
        joined.update(captured)
    Path(out).write_text(json.dumps(joined, sort_keys=True))
    print(f"joined {len(joined)} runs from {len(parts)} parts to {out}")


# A release number, with an optional prerelease such as `rc1` (PEP 440's short form)
_RELEASE = re.compile(r"(\d+(?:\.\d+)*)(?:(a|b|rc)(\d+))?")
# A prerelease orders before its release: alpha, beta, release candidate, release
_STAGES = {"a": 0, "b": 1, "rc": 2, None: 3}


def _version(text: str) -> Tuple[Tuple[int, ...], int, int]:
    """A release number such as `0.23.0` or `1.0.0rc1` as a tuple that orders
    correctly, a prerelease before its release.

    Args:
        text: The release number.

    Returns:
        Tuple[Tuple[int, ...], int, int]: The release's numbers, its stage and the
        prerelease's number.

    Raises:
        ValueError: If the text is not a release number.
    """
    match = _RELEASE.fullmatch(str(text))
    if match is None:
        raise ValueError(f"not a release number: {text!r}")
    release, stage, number = match.groups()
    numbers = tuple(int(part) for part in release.split("."))
    return numbers, _STAGES[stage], int(number or 0)


def _expected(path: Path, baseline: str) -> Dict[str, Dict[str, Any]]:
    """The entries of `expected.yaml` that apply to a baseline, by run key. An entry
    applies when its `since` is above the baseline."""
    entries = yaml.safe_load(path.read_text()) or []
    for entry in entries:
        missing = {"run", "since", "changelog"} - set(entry)
        if missing:
            raise SystemExit(
                f"{path}: entry {entry.get('run')} lacks {sorted(missing)}"
            )
    for entry in entries:
        if not isinstance(entry["since"], str):
            raise SystemExit(
                f"{path}: entry {entry['run']} has since {entry['since']!r}, which "
                "YAML reads as a number; write it in quotes, such as '1.10'"
            )
    runs = [entry["run"] for entry in entries]
    repeated = sorted({run for run in runs if runs.count(run) > 1})
    if repeated:
        raise SystemExit(f"{path}: more than one entry for {repeated}")
    return {
        entry["run"]: entry
        for entry in entries
        if _version(entry["since"]) > _version(baseline)
    }


def compare(
    old: str,
    new: str,
    baseline: Optional[str] = None,
    expected: Path = EXPECTED,
    set_aside_absent: bool = False,
) -> int:
    """Compares two capture files and prints each difference.

    Args:
        old: The capture of the baseline release.
        new: The capture of the checkout under test.
        baseline: The baseline release whose listed differences are expected.
        expected: The file that lists the expected differences.
        set_aside_absent: Set aside, before the stale check, each listed entry whose
            run is in neither capture. The comparison of the files of one release
            needs it, because an entry for a file added in a later release has no run
            there.

    Returns:
        int: 1 when a difference is not expected or an expected one is missing, else 0.
    """
    before = json.loads(Path(old).read_text())
    after = json.loads(Path(new).read_text())
    listed = _expected(expected, baseline) if baseline else {}
    if set_aside_absent:
        listed = {k: v for k, v in listed.items() if k in before or k in after}
    added, failures, matched = [], [], set()
    for key in sorted(set(before) | set(after)):
        if key not in before:
            problem = f"{key}: only in {new}"
        elif key not in after:
            problem = f"{key}: only in {old}"
        elif before[key] != after[key]:
            problem = f"{key}: records differ"
        else:
            continue
        if baseline and key not in before:
            added.append(problem)
        elif key in listed:
            matched.add(key)
        else:
            failures.append(problem)
    if added:
        print(f"{len(added)} runs added:")
        for line in added:
            print(f"  {line}")
    stale = sorted(set(listed) - matched)
    if stale:
        print(f"{len(stale)} expected entries match no difference:")
        for key in stale:
            print(f"  {key}")
    if failures:
        print(f"{len(failures)} differences:")
        for line in failures:
            print(f"  {line}")
    if failures or stale:
        return 1
    print(
        f"{len(matched)} expected differences, no others, across {len(after)} runs"
        if baseline
        else f"no differences across {len(after)} runs"
    )
    return 0


def main() -> int:
    """Runs the `capture` or `compare` command from the command line.

    Returns:
        int: The exit status of the command.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    cap = commands.add_parser("capture", help="capture a checkout's runs to a file")
    cap.add_argument("--root", required=True)
    cap.add_argument("--out", required=True)
    cap.add_argument(
        "--scenarios",
        help="the scenarios folder (default: <root>/tests/regression/scenarios)",
    )
    cap.add_argument(
        "--left-out",
        help="the list of left-out files (default: <root>/tests/regression/left_out.yaml)",
    )
    cap.add_argument(
        "--shard",
        default="1/1",
        help="capture one part of the runs, such as 2/6; `merge` joins the parts",
    )
    mrg = commands.add_parser("merge", help="join the capture files of the parts")
    mrg.add_argument("--out", required=True)
    mrg.add_argument("parts", nargs="+")
    cmp = commands.add_parser("compare", help="compare two capture files")
    cmp.add_argument("old")
    cmp.add_argument("new")
    cmp.add_argument(
        "--baseline", help="the release the old capture ran, such as 0.22.0"
    )
    cmp.add_argument("--expected", type=Path, default=EXPECTED)
    cmp.add_argument(
        "--set-aside-absent",
        action="store_true",
        help="set aside the entries whose run is in neither capture",
    )
    args = parser.parse_args()
    if args.command == "capture":
        global SHARD
        part, count = (int(n) for n in args.shard.split("/"))
        if not 1 <= part <= count:
            raise SystemExit(f"--shard {args.shard}: the part must be 1 to {count}")
        SHARD = (part, count)
        capture(args.root, args.out, args.scenarios, args.left_out)
        return 0
    if args.command == "merge":
        merge(args.parts, args.out)
        return 0
    return compare(
        args.old, args.new, args.baseline, args.expected, args.set_aside_absent
    )


if __name__ == "__main__":
    raise SystemExit(main())
