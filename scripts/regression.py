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
        --out /tmp/old.json
    uv run --all-extras python scripts/regression.py compare /tmp/old.json new.json \\
        --baseline 0.22.0

Run the old side from a directory outside the repository, such as `/tmp`, and name
the working tree's script by its absolute path. From inside the repository,
`uv run --no-project` imports the working tree's desflow, not the released wheel
(`desflow.__file__` is then `src/desflow/__init__.py`; from `/tmp` it is the PyPI
package).

`--root` is put first on `sys.path`, with its `tests/docs`, so the capture imports
that checkout's twin-test runners and its `tests.conformance`; the desflow it runs is
the one installed. Records are compared without `timestamp` and `system.` paths, as
the twin test compares them.

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
entry that matches no difference is reported as stale.
"""

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import yaml

EXPECTED = (
    Path(__file__).resolve().parents[1] / "tests" / "regression" / "expected.yaml"
)


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
        try:
            records, _ = harness.run_yaml(path.name)
            captured[f"blueprint:{path.name}"] = twins._records(records)
        except Exception as exc:  # noqa: BLE001: the exception is the captured value
            captured[f"blueprint:{path.name}"] = {
                "error": f"{type(exc).__name__}: {exc}"
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


def capture(root: str, out: str, scenarios: Optional[str] = None) -> None:
    root_path = Path(root).resolve()
    twins, harness = _load(root_path)
    folder = Path(scenarios or root_path / "tests" / "regression" / "scenarios")
    if not folder.is_dir():
        raise SystemExit(f"no scenarios folder at {folder}; pass --scenarios")
    with patch("pyiceberg.catalog.rest.RestCatalog", MagicMock()):
        captured = {**_examples(twins), **_blueprints(root_path, twins, harness)}
        captured.update(_scenarios(twins, folder.resolve()))
    Path(out).write_text(json.dumps(captured, default=str, sort_keys=True))
    print(f"captured {len(captured)} runs from {root_path} to {out}")


def _version(text: str) -> Tuple[int, ...]:
    """A release number such as `0.23.0` as a tuple that orders correctly."""
    return tuple(int(part) for part in str(text).split("."))


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
    old: str, new: str, baseline: Optional[str] = None, expected: Path = EXPECTED
) -> int:
    before = json.loads(Path(old).read_text())
    after = json.loads(Path(new).read_text())
    listed = _expected(expected, baseline) if baseline else {}
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
        return 1
    print(
        f"{len(matched)} expected differences, no others, across {len(after)} runs"
        if baseline
        else f"no differences across {len(after)} runs"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    cap = commands.add_parser("capture", help="capture a checkout's runs to a file")
    cap.add_argument("--root", required=True)
    cap.add_argument("--out", required=True)
    cap.add_argument(
        "--scenarios",
        help="the scenarios folder (default: <root>/tests/regression/scenarios)",
    )
    cmp = commands.add_parser("compare", help="compare two capture files")
    cmp.add_argument("old")
    cmp.add_argument("new")
    cmp.add_argument(
        "--baseline", help="the release the old capture ran, such as 0.22.0"
    )
    cmp.add_argument("--expected", type=Path, default=EXPECTED)
    args = parser.parse_args()
    if args.command == "capture":
        capture(args.root, args.out, args.scenarios)
        return 0
    return compare(args.old, args.new, args.baseline, args.expected)


if __name__ == "__main__":
    raise SystemExit(main())
