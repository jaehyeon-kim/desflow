"""Regression capture and compare against a baseline release.

Layer 3 of the 0.23.0 regression guard (see release/0.23.0/progress.md). It runs every
example and every conformance blueprint unpaced, captures every event, label and
telemetry record, and compares a baseline capture with the working tree's, field by
field. Each side runs its own files, so a changed example, runner or blueprint never
runs on the baseline:

    uv run --all-extras python scripts/regression.py capture --root . --out new.json
    cd /tmp && uv run --no-project --python 3.12 --with "desflow[all]==0.22.0" \\
        --with pytest python <repository>/scripts/regression.py capture \\
        --root <v0.22.0 checkout> --out /tmp/old.json
    uv run --all-extras python scripts/regression.py compare /tmp/old.json new.json

Run the old side from a directory outside the repository, such as `/tmp`, and name
the working tree's script by its absolute path. From inside the repository,
`uv run --no-project` imports the working tree's desflow, not the released wheel
(`desflow.__file__` is then `src/desflow/__init__.py`; from `/tmp` it is the PyPI
package).

`--root` is put first on `sys.path`, with its `tests/docs`, so the capture imports
that checkout's twin-test runners and its `tests.conformance`; the desflow it runs is
the one installed. Records are compared without `timestamp` and `system.` paths, as
the twin test compares them. Every difference must be one the release notes list; any
other difference fails `compare`.
"""

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch


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


def capture(root: str, out: str) -> None:
    root_path = Path(root).resolve()
    twins, harness = _load(root_path)
    with patch("pyiceberg.catalog.rest.RestCatalog", MagicMock()):
        captured = {**_examples(twins), **_blueprints(root_path, twins, harness)}
    Path(out).write_text(json.dumps(captured, default=str, sort_keys=True))
    print(f"captured {len(captured)} runs from {root_path} to {out}")


def compare(old: str, new: str) -> int:
    before = json.loads(Path(old).read_text())
    after = json.loads(Path(new).read_text())
    differences = []
    for key in sorted(set(before) | set(after)):
        if key not in before:
            differences.append(f"{key}: only in {new}")
        elif key not in after:
            differences.append(f"{key}: only in {old}")
        elif before[key] != after[key]:
            differences.append(f"{key}: records differ")
    if differences:
        print(f"{len(differences)} differences:")
        for line in differences:
            print(f"  {line}")
        return 1
    print(f"no differences across {len(after)} runs")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    cap = commands.add_parser("capture", help="capture a checkout's runs to a file")
    cap.add_argument("--root", required=True)
    cap.add_argument("--out", required=True)
    cmp = commands.add_parser("compare", help="compare two capture files")
    cmp.add_argument("old")
    cmp.add_argument("new")
    args = parser.parse_args()
    if args.command == "capture":
        capture(args.root, args.out)
        return 0
    return compare(args.old, args.new)


if __name__ == "__main__":
    raise SystemExit(main())
