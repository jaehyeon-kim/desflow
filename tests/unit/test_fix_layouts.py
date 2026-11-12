"""`desflow test --fix` writes each measured value into its own key, in every layout.

The tests of the capture blueprint that has them (`tests/conformance/yaml_only/
b3_tests_and_sinks.yaml`) and of three layouts of `expect` that differ from the
measured values are fixed, and the file read back must give the measured values.
"""

import textwrap
from pathlib import Path
from typing import List, Mapping

import pytest

from desflow.blueprint import BlueprintError
from desflow.blueprint.build import read_tests_file
from desflow.blueprint.expectations import fix_expectations, holds

ROOT = Path(__file__).resolve().parents[2]

LAYOUTS = {
    "block": """\
        tests:
          - name: t
            expect:
              a: {value: 1, within: 0}
              b: {value: 2, within: 0}
        """,
    "flow": """\
        tests:
          - name: t
            expect: {a: 1, b: 2}
        """,
    "one line": """\
        tests:
          - name: t
            expect: {a: {value: 1, within: 0}, b: {value: 2, within: 0}}
        """,
    "flow over lines": """\
        tests:
          - name: t
            expect: {
              a: 5,
              b: 7 }
        """,
    "quoted names": """\
        tests:
          - name: t
            expect: {"a": {value: 1, within: 0}, 'b': {value: 2, within: 0}}
        """,
    "key before a value key": """\
        tests:
          - name: t
            expect: {value: {value: 1, within: 0}, b: {value: 2, within: 0}}
        """,
}


def _fix(tmp_path: Path, text: str, measured: List[Mapping[str, float]]) -> Path:
    path = tmp_path / "tests.yaml"
    path.write_text(textwrap.dedent(text), encoding="utf-8")
    tests, source = read_tests_file(path)
    fixed = fix_expectations(path.read_text(encoding="utf-8"), source, tests, measured)
    path.write_text(fixed, encoding="utf-8")
    return path


@pytest.mark.parametrize("layout", sorted(LAYOUTS))
def test_fix_writes_the_measured_value_into_the_key_it_belongs_to(layout, tmp_path):
    first = "value" if layout == "key before a value key" else "a"
    measured = {first: 1, "b": 9}
    path = _fix(tmp_path, LAYOUTS[layout], [measured])
    tests, _ = read_tests_file(path)
    for name, value in measured.items():
        assert holds(tests[0].expect[name], value), (
            f"{layout}: {name} should hold {value}\n{path.read_text()}"
        )


def test_fix_changes_the_first_of_two_keys_alone(tmp_path):
    path = _fix(tmp_path, LAYOUTS["one line"], [{"a": 5, "b": 2}])
    assert "a: {value: 5, within: 0}, b: {value: 2, within: 0}" in path.read_text()


def test_fix_writes_an_empty_expect_that_is_the_first_key_of_a_test(tmp_path):
    path = _fix(
        tmp_path,
        """\
        tests:
          - expect: {}
            name: t
        """,
        [{"a": 3}],
    )
    tests, _ = read_tests_file(path)
    assert holds(tests[0].expect["a"], 3), path.read_text()


def test_fix_refuses_an_expect_that_two_tests_share_through_an_anchor(tmp_path):
    with pytest.raises(BlueprintError) as error:
        _fix(
            tmp_path,
            """\
            tests:
              - name: one
                expect: &e
                  a: 5
              - name: two
                expect: *e
            """,
            [{"a": 5}, {"a": 9}],
        )
    message = str(error.value)
    assert message.startswith(f"{tmp_path / 'tests.yaml'}:4: "), message
    assert "tests 'one', 'two' share" in message


def test_the_capture_blueprint_with_tests_is_fixed_to_its_measured_values(tmp_path):
    from desflow.blueprint.build import load_blueprint

    original = ROOT / "tests" / "conformance" / "yaml_only" / "b3_tests_and_sinks.yaml"
    path = tmp_path / "model.yaml"
    path.write_text(original.read_text(encoding="utf-8"), encoding="utf-8")
    built = load_blueprint(path, None, None)
    measured = [
        {
            name: (expected.value if hasattr(expected, "value") else expected) + 1
            for name, expected in test.expect.items()
        }
        for test in built.blueprint.tests
    ]
    text = path.read_text(encoding="utf-8")
    fixed = fix_expectations(text, built.source, built.blueprint.tests, measured)
    assert fixed != text
    path.write_text(fixed, encoding="utf-8")
    for test, found in zip(load_blueprint(path, None, None).blueprint.tests, measured):
        for name, value in found.items():
            assert holds(test.expect[name], value), name
