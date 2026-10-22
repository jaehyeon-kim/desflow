"""`scripts/regression.py compare` and `capture` fail and pass as their rules say.

The captures are small files built here, so no model runs. Each test checks one rule of
`compare --baseline`: which differences fail, which are reported, and which
`expected.yaml` files are refused.
"""

import importlib.util
import json
from pathlib import Path

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


def _compare(tmp_path, old, new, entries=None, baseline="0.22.0"):
    (tmp_path / "old.json").write_text(json.dumps(old))
    (tmp_path / "new.json").write_text(json.dumps(new))
    expected = tmp_path / "expected.yaml"
    expected.write_text(yaml.safe_dump(entries or []))
    return regression.compare(
        str(tmp_path / "old.json"),
        str(tmp_path / "new.json"),
        baseline,
        expected,
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


def test_an_entry_that_matches_no_difference_is_reported_as_stale(tmp_path, capsys):
    captures = {"example:a:yaml": SAME}
    assert _compare(tmp_path, captures, captures, [_entry("example:a:yaml")]) == 0
    out = capsys.readouterr().out
    assert "1 expected entries match no difference" in out
    assert "example:a:yaml" in out


def test_an_entry_without_a_changelog_line_is_refused(tmp_path):
    captures = {"example:a:yaml": SAME}
    with pytest.raises(SystemExit, match=r"lacks \['changelog'\]"):
        _compare(
            tmp_path, captures, captures, [_entry("example:a:yaml", changelog=None)]
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
