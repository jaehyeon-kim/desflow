"""`scripts/regression.py compare` and `capture` fail and pass as their rules say.

The captures are small files built here, so no model runs. Each test checks one rule of
`compare --baseline`: which differences fail, which are reported, and which
`expected.yaml` files are refused.
"""

import importlib.util
import json
import signal
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "regression.py"
_spec = importlib.util.spec_from_file_location("regression_script", _SCRIPT)
assert _spec is not None and _spec.loader is not None
regression = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(regression)

ROOT = Path(__file__).resolve().parents[2]
SAME = [{"path_id": "Shop.a", "value": 1}]
OTHER = [{"path_id": "Shop.a", "value": 2}]


def _entry(run, since="0.23.0", changelog="A line of the changelog."):
    entry = {"run": run, "since": since}
    if changelog is not None:
        entry["changelog"] = changelog
    return entry


def _compare(
    tmp_path, old, new, entries=None, baseline="0.22.0", set_aside_absent=False
):
    (tmp_path / "old.json").write_text(json.dumps(old))
    (tmp_path / "new.json").write_text(json.dumps(new))
    expected = tmp_path / "expected.yaml"
    expected.write_text(yaml.safe_dump(entries or []))
    return regression.compare(
        str(tmp_path / "old.json"),
        str(tmp_path / "new.json"),
        baseline,
        expected,
        set_aside_absent,
    )


def test_equal_captures_pass_with_and_without_a_baseline(tmp_path, capsys):
    captures = {"example:a:yaml": SAME}
    assert _compare(tmp_path, captures, captures) == 0
    assert "0 expected differences, no others, across 1 runs" in capsys.readouterr().out
    assert _compare(tmp_path, captures, captures, baseline=None) == 0
    assert "no differences across 1 runs" in capsys.readouterr().out


def test_a_run_only_in_the_new_capture_is_added_with_a_baseline(tmp_path, capsys):
    old, new = (
        {"example:a:yaml": SAME},
        {"example:a:yaml": SAME, "example:b:yaml": SAME},
    )
    assert _compare(tmp_path, old, new) == 0
    out = capsys.readouterr().out
    assert "1 runs added" in out
    assert "example:b:yaml: only in" in out


def test_a_run_only_in_the_new_capture_fails_without_a_baseline(tmp_path):
    old, new = (
        {"example:a:yaml": SAME},
        {"example:a:yaml": SAME, "example:b:yaml": SAME},
    )
    assert _compare(tmp_path, old, new, baseline=None) == 1


def test_a_run_only_in_the_old_capture_fails_unless_listed(tmp_path, capsys):
    old, new = (
        {"example:a:yaml": SAME, "example:b:yaml": SAME},
        {"example:a:yaml": SAME},
    )
    assert _compare(tmp_path, old, new) == 1
    assert "example:b:yaml: only in" in capsys.readouterr().out
    assert _compare(tmp_path, old, new, [_entry("example:b:yaml")]) == 0


def test_different_records_fail_unless_listed(tmp_path, capsys):
    old, new = {"example:a:yaml": SAME}, {"example:a:yaml": OTHER}
    assert _compare(tmp_path, old, new) == 1
    assert "example:a:yaml: records differ" in capsys.readouterr().out
    assert _compare(tmp_path, old, new, [_entry("example:a:yaml")]) == 0
    assert "1 expected differences, no others" in capsys.readouterr().out


def test_an_entry_for_one_run_does_not_cover_another(tmp_path):
    old = {"example:a:yaml": SAME, "example:b:yaml": SAME}
    new = {"example:a:yaml": OTHER, "example:b:yaml": OTHER}
    assert _compare(tmp_path, old, new, [_entry("example:a:yaml")]) == 1


def test_an_entry_whose_since_is_not_above_the_baseline_does_not_apply(tmp_path):
    old, new = {"example:a:yaml": SAME}, {"example:a:yaml": OTHER}
    for since in ("0.22.0", "0.21.0"):
        assert _compare(tmp_path, old, new, [_entry("example:a:yaml", since)]) == 1
    assert _compare(tmp_path, old, new, [_entry("example:a:yaml", "0.22.1")]) == 0


def test_a_prerelease_baseline_orders_before_its_release(tmp_path):
    old, new = {"example:a:yaml": SAME}, {"example:a:yaml": OTHER}
    entry = _entry("example:a:yaml", "1.0.0rc1")
    assert _compare(tmp_path, old, new, [entry], baseline="1.0.0rc1") == 1
    entry = _entry("example:a:yaml", "1.0.0")
    assert _compare(tmp_path, old, new, [entry], baseline="1.0.0rc1") == 0


def test_an_entry_that_matches_no_difference_fails_as_stale(tmp_path, capsys):
    captures = {"example:a:yaml": SAME}
    assert _compare(tmp_path, captures, captures, [_entry("example:a:yaml")]) == 1
    out = capsys.readouterr().out
    assert "1 expected entries match no difference" in out
    assert "example:a:yaml" in out


def test_an_entry_for_a_run_in_neither_capture_is_set_aside_only_when_asked(tmp_path):
    # a file added in 1.1.0 and changed on purpose in 1.2.0 has no run among the files
    # of v1.0.0, so the comparison of those files must not call its entry stale
    captures = {"example:a:yaml": SAME}
    entries = [_entry("file:docs/added_in_1_1.yaml", "1.2.0")]
    assert _compare(tmp_path, captures, captures, entries, "1.0.0") == 1
    assert _compare(tmp_path, captures, captures, entries, "1.0.0", True) == 0


def test_set_aside_keeps_the_stale_check_for_a_run_both_captures_hold(tmp_path):
    captures = {"example:a:yaml": SAME}
    entries = [_entry("example:a:yaml", "1.2.0")]
    assert _compare(tmp_path, captures, captures, entries, "1.0.0", True) == 1


def test_an_entry_without_a_changelog_line_is_refused(tmp_path):
    captures = {"example:a:yaml": SAME}
    with pytest.raises(SystemExit, match=r"lacks \['changelog'\]"):
        _compare(
            tmp_path, captures, captures, [_entry("example:a:yaml", changelog=None)]
        )


def test_a_since_that_yaml_reads_as_a_number_is_refused(tmp_path):
    # `since: 1.10` is the number 1.1 in YAML, so it would never apply above 1.2.0
    captures = {"example:a:yaml": SAME}
    (tmp_path / "old.json").write_text(json.dumps(captures))
    expected = tmp_path / "expected.yaml"
    expected.write_text("- {run: 'example:a:yaml', since: 1.10, changelog: A line.}\n")
    with pytest.raises(SystemExit, match=r"example:a:yaml.*since.*quote"):
        regression.compare(
            str(tmp_path / "old.json"), str(tmp_path / "old.json"), "1.2.0", expected
        )


def test_two_entries_for_one_run_are_refused(tmp_path):
    old, new = {"example:a:yaml": SAME}, {"example:a:yaml": OTHER}
    entries = [_entry("example:a:yaml"), _entry("example:a:yaml")]
    with pytest.raises(SystemExit, match="more than one entry"):
        _compare(tmp_path, old, new, entries)


def test_capture_without_a_scenarios_folder_names_it(tmp_path):
    with pytest.raises(SystemExit, match="no scenarios folder at .*; pass --scenarios"):
        regression.capture(
            str(ROOT), str(tmp_path / "out.json"), str(tmp_path / "missing")
        )


def _tree(root, names):
    for name in names:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("simulation: {}\n")


def _twins(root, load=None):
    return SimpleNamespace(
        EXAMPLES=root / "examples",
        EXAMPLE_CASES=[("a", "a.yaml", None, 1, None)],
        _from_yaml=load or (lambda path: path),
        _run_builder=lambda app, until: [{"file": app.name}],
    )


def test_all_blueprints_runs_the_files_no_other_capture_runs(tmp_path):
    _tree(
        tmp_path,
        [
            "examples/yaml/a.yaml",  # an example, run by `_examples`
            "examples/yaml/b.yaml",
            "tests/conformance/blueprints/c.yaml",  # run by `_blueprints`
            "tests/regression/scenarios/yaml/s.yaml",  # run by `_scenarios`
            "docs/snippets/d.yml",
            "docs/snippets/f.YAML",  # a suffix in capitals is a blueprint too
            "tests/known_cases/x/model.yaml",
            "tests/known_cases/index.yaml",  # left out
            "src/e.yaml",  # outside the three folders
        ],
    )
    left_out = {"tests/known_cases/index.yaml": "a list, not a blueprint"}
    names = [
        path.relative_to(tmp_path).as_posix()
        for path in regression._other_blueprints(tmp_path, _twins(tmp_path), left_out)
    ]
    assert names == [
        "docs/snippets/d.yml",
        "docs/snippets/f.YAML",
        "examples/yaml/b.yaml",
        "tests/known_cases/x/model.yaml",
    ]


def test_a_module_imported_by_one_captured_file_is_not_visible_to_the_next(tmp_path):
    """Each file runs with the `sys.modules` and `sys.path` it started with, so a
    `!python` module that `a` imports and finds beside it is not found by `b`."""
    _tree(tmp_path, ["docs/a/a.yaml", "docs/b/b.yaml"])
    (tmp_path / "docs" / "a" / "leaked_by_a.py").write_text("VALUE = 1\n")
    before = (dict(sys.modules), list(sys.path))

    def load(
        path,
    ):  # as the loader does for `!python`: the folder first, then the import
        sys.path.insert(0, str(path.parent))
        importlib.import_module("leaked_by_a")
        return SimpleNamespace(name=path.name)

    captured = regression._all_blueprints(tmp_path, _twins(tmp_path, load), {})
    assert captured == {
        "file:docs/a/a.yaml": [{"file": "a.yaml"}],
        "file:docs/b/b.yaml": {
            "error": "ModuleNotFoundError: No module named 'leaked_by_a'"
        },
    }
    assert "leaked_by_a" not in sys.modules
    assert (dict(sys.modules), list(sys.path)) == before


def test_all_blueprints_keeps_a_refusal_as_its_error(tmp_path):
    _tree(tmp_path, ["docs/d.yaml", "docs/e.yaml"])

    def load(path):
        if path.name == "d.yaml":
            raise ValueError("unknown key 'x'")
        return SimpleNamespace(name=path.name)

    captured = regression._all_blueprints(tmp_path, _twins(tmp_path, load), {})
    assert captured == {
        "file:docs/d.yaml": {"error": "ValueError: unknown key 'x'"},
        "file:docs/e.yaml": [{"file": "e.yaml"}],
    }


@pytest.mark.skipif(
    not hasattr(signal, "SIGALRM"), reason="the time limit needs SIGALRM"
)
def test_all_blueprints_stops_and_names_a_file_that_does_not_end(tmp_path):
    _tree(tmp_path, ["docs/a.yaml", "docs/b.yaml"])
    twins = _twins(tmp_path)

    def block(app, until):
        if app.name == "b.yaml":
            time.sleep(10)
        return [{"file": app.name}]

    twins._run_builder = block
    twins._from_yaml = lambda path: SimpleNamespace(name=path.name)
    with pytest.raises(
        SystemExit, match=r"docs/b.yaml did not end within 1 s; give it"
    ):
        regression._all_blueprints(tmp_path, twins, {}, seconds=1)


def test_a_left_out_entry_needs_a_reason_and_the_list_must_exist(tmp_path):
    with pytest.raises(SystemExit, match="no list of left-out files"):
        regression._left_out(tmp_path / "missing.yaml")
    path = tmp_path / "left_out.yaml"
    path.write_text("docs/a.yaml: a reason\ndocs/b.yaml: ''\n")
    with pytest.raises(SystemExit, match=r"no reason for \['docs/b.yaml'\]"):
        regression._left_out(path)


def test_every_left_out_file_of_the_repository_exists_and_has_a_reason():
    left_out = regression._left_out(ROOT / "tests" / "regression" / "left_out.yaml")
    assert left_out
    missing = [name for name in left_out if not (ROOT / name).is_file()]
    assert not missing, f"left out but not a file: {missing}"
