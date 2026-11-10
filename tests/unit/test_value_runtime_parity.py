"""`ValueRuntime.value` does what a `Template` does with the same generator.

Called three times with one generator made by `generator(...)`, `value` must give what
a `Template` of that generator gives rendered three times: a `sequence` counts, a
`unique` choice refuses once its choices are used, and a `row` generator reads its file
from the working folder. A random generator gives the same three values in two runs
with one seed, and values of its own kind. A mapping with a `gen` key is refused, with
a message that says to use `generator(...)`.
"""

import contextlib
import os
from pathlib import Path
from typing import Any, List

import pytest

from desflow.core.environment import DynamicRealtimeEnvironment
from desflow.values import Template, ValueRuntime, generator

ROOT = Path(__file__).resolve().parents[2]

# Generators that need no task, entity or other generator to give a value
DETERMINISTIC = {
    "sequence": {"gen": "sequence"},
    "sequence_step": {"gen": "sequence", "start": 5, "step": 2},
    "row_sequential": {
        "gen": "row",
        "file": "examples/local_replay_orders.csv",
        "order": "sequential",
    },
    "literal": {"gen": "literal", "value": {"a": 1}},
    "expr": {"gen": "expr", "expr": "1 + 2"},
    "add": {"gen": "add", "args": [1, 2]},
    "upper": {"gen": "upper", "value": "ab"},
    "format": {"gen": "format", "template": "x{n}", "names": {"n": 3}},
}
RANDOM = {
    "row": {"gen": "row", "file": "examples/local_replay_orders.csv"},
    "uniform": {"gen": "uniform", "low": 0, "high": 100},
    "normal": {"gen": "normal", "mean": 5, "std": 1},
    "integer": {"gen": "integer", "bounds": [0, 1000]},
    "one_of": {"gen": "one_of", "choices": [1, 2, 3, 4, 5, 6]},
    "weighted": {"gen": "weighted", "options": {"a": 1, "b": 2}},
    "uuid": {"gen": "uuid"},
    "boolean": {"gen": "boolean"},
}


@contextlib.contextmanager
def _from_root():
    old = Path.cwd()
    os.chdir(ROOT)
    try:
        yield
    finally:
        os.chdir(old)


def _runtime(seed: int = 7) -> ValueRuntime:
    return ValueRuntime(seed, DynamicRealtimeEnvironment(factor=0))


def _template_values(spec: Any) -> List[Any]:
    template = Template({"v": spec}, name="t")
    runtime = _runtime()
    return [(runtime.render(template) or {})["v"] for _ in range(3)]


@pytest.mark.parametrize("name", sorted(DETERMINISTIC))
def test_value_gives_what_a_template_gives_for_the_same_generator(name):
    spec = DETERMINISTIC[name]
    with _from_root():
        runtime = _runtime()
        made = generator(spec)
        values = [runtime.value(made) for _ in range(3)]
        expected = _template_values(spec)
    assert values == expected, f"{name}: value gave {values}, a Template {expected}"


@pytest.mark.parametrize("name", sorted(RANDOM))
def test_a_random_generator_gives_three_values_that_repeat_with_the_seed(name):
    spec = RANDOM[name]

    def three() -> List[Any]:
        with _from_root():
            runtime = _runtime()
            made = generator(spec)
            return [runtime.value(made) for _ in range(3)]

    first, second = three(), three()
    assert first == second, name
    if name in ("uniform", "normal", "integer", "uuid"):
        assert len(set(map(str, first))) == 3, f"{name}: {first}"


def test_a_unique_choice_refuses_when_its_choices_are_used():
    spec = {"gen": "one_of", "choices": [1, 2], "unique": True}
    runtime = _runtime()
    made = generator(spec)
    got = sorted([runtime.value(made), runtime.value(made)])
    assert got == [1, 2]
    with pytest.raises(Exception, match="unique"):
        runtime.value(made)
    template = Template({"v": spec}, name="t")
    other = _runtime()
    other.render(template)
    other.render(template)
    with pytest.raises(Exception, match="unique"):
        other.render(template)


def test_a_row_file_is_read_from_the_working_folder():
    with _from_root():
        row = _runtime().value(
            generator({"gen": "row", "file": "examples/local_replay_orders.csv"})
        )
    assert set(row) >= {"time", "order_id"}, row


def test_a_row_file_that_does_not_exist_is_refused_with_its_name(tmp_path):
    made = generator({"gen": "row", "file": "missing_rows.csv"})
    with pytest.raises(ValueError, match="missing_rows.csv"):
        _runtime().value(made)


@pytest.mark.parametrize("name", ["sequence", "one_of"])
def test_a_mapping_is_refused_with_the_way_to_make_a_generator(name):
    spec = {**DETERMINISTIC, **RANDOM}[name]
    with pytest.raises(ValueError, match=r"generator\(\.\.\.\)"):
        _runtime().value(spec)
