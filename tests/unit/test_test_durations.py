"""scripts/test_durations.py reads the JUnit files of a CI run into test times."""

import importlib.util
import json
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "test_durations.py"
_spec = importlib.util.spec_from_file_location("test_durations", SCRIPT)
assert _spec is not None and _spec.loader is not None
test_durations = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(test_durations)

JUNIT = """<testsuites><testsuite>
<testcase classname="tests.unit.test_ci_plan" name="test_a" time="2.0"/>
<testcase classname="" name="tests.unit.test_skipped_module" time="0.000"/>
</testsuite></testsuites>
"""


def test_a_module_level_skip_is_passed_over(tmp_path, monkeypatch):
    (tmp_path / "junit").mkdir()
    (tmp_path / "junit" / "a.xml").write_text(JUNIT)
    out = tmp_path / "durations.json"
    monkeypatch.setattr(test_durations, "OUT", out)
    test_durations.main(str(tmp_path / "junit"))
    assert json.loads(out.read_text()) == {
        "__other__": 0.0,
        "tests/unit/test_ci_plan.py::test_a": 2.0,
    }
