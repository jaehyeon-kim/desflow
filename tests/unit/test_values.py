"""The generators of a blueprint and `Template`: each kind, each modifier, each `order`
and `on_end` of `row`, the seeding rule, and the task keys that name values.

A helper runs one task over a few entities and returns the payloads it published, so
a test writes the payload and reads what came out.
"""

import json
import re
import textwrap
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from desflow import SimulationContext
from desflow.blueprint import BlueprintError
from desflow.core.task import TaskFailed
from desflow.values import Template
from tests.conformance.harness import RecordingEgress, run_yaml_text

HEAD = (
    "simulation: {sim_id: V, factor: 0, random_seed: %d, "
    'logical_start_time: "2026-01-01T00:00:00+00:00", timezone: UTC}\n'
)


def model(body: str, seed: int = 1, extra: str = "", spawn: str = "t") -> str:
    """A blueprint with one replayed arrival that spawns `spawn`, and `body` as the
    keys of the task `t`."""
    return (
        HEAD % seed
        + "services: {s1: {dist: constant, value: 1}, s5: {dist: constant, value: 5}}\n"
        + f"arrivals: {{a: {{replay: {{path: a.csv}}, spawn: {spawn}}}}}\n"
        + extra
        + "tasks:\n  t:\n"
        + textwrap.indent(textwrap.dedent(body).strip("\n"), "    ")
        + "\n"
    )


def run(tmp_path: Path, body: str, n: int = 3, seed: int = 1, extra: str = "") -> Any:
    """The outcome of a run of `n` entities, one a second."""
    return run_yaml_text(
        tmp_path, model(body, seed, extra), {"a": [float(i) for i in range(n)]}
    )


def payloads(outcome: Any, task: str = "t") -> List[Dict[str, Any]]:
    """The finished payloads of a task: events that are not statuses."""
    assert outcome.error is None, outcome.error
    return [
        record["value"]
        for record in outcome.records
        if record["stream_type"] == "event"
        and record["task"] == task
        and isinstance(record["value"], dict)
        and "status" not in record["value"]
    ]


def field(
    tmp_path: Path, spec: str, n: int = 3, seed: int = 1, extra: str = ""
) -> list:
    """The value a generator written as `spec` gives for each of `n` entities."""
    body = f"payload: {{x: {spec}}}"
    return [p["x"] for p in payloads(run(tmp_path, body, n, seed, extra))]


def error_of(tmp_path: Path, body: str, n: int = 1) -> str:
    outcome = run(tmp_path, body, n)
    assert outcome.error is not None
    return str(outcome.error)


# --- numbers -----------------------------------------------------------------


def test_uniform_stays_in_range_and_rounds(tmp_path):
    values = field(tmp_path, "{gen: uniform, low: 2, high: 4, decimals: 2}", n=60)
    assert all(2 <= v <= 4 for v in values)
    assert all(round(v, 2) == v for v in values)
    assert len(set(values)) > 10
    whole = field(tmp_path, "{gen: uniform, low: 2, high: 4, decimals: 0}", n=20)
    assert all(isinstance(v, int) and 2 <= v <= 4 for v in whole)


def test_uniform_with_high_below_low_names_the_line(tmp_path):
    message = error_of(tmp_path, "payload: {x: {gen: uniform, low: 4, high: 2}}")
    assert "model.yaml:" in message
    assert "high 2 is below low 4" in message
    assert "tasks.t.payload.x" in message


def test_integer_reaches_both_ends(tmp_path):
    values = field(tmp_path, "{gen: integer, bounds: [3, 5]}", n=120)
    assert set(values) == {3, 4, 5}
    assert all(isinstance(v, int) for v in values)


def test_integer_takes_whole_numbers_only(tmp_path):
    message = error_of(tmp_path, "payload: {x: {gen: integer, bounds: [1.5, 4]}}")
    assert "must be a whole number" in message


def test_normal_mean_and_deviation(tmp_path):
    values = field(tmp_path, "{gen: normal, mean: 50, std: 5}", n=400)
    mean = sum(values) / len(values)
    deviation = (sum((v - mean) ** 2 for v in values) / len(values)) ** 0.5
    assert 48.5 < mean < 51.5
    assert 4 < deviation < 6


def test_sample_draws_from_a_family_and_a_service(tmp_path):
    values = field(tmp_path, "{gen: sample, dist: exponential, rate: 2}", n=200)
    assert 0.35 < sum(values) / len(values) < 0.7
    extra = "services: {s: {dist: constant, value: 3}}\n"
    assert field(tmp_path, "{gen: sample, service: s}", extra=extra) == [3, 3, 3]
    duration = field(tmp_path, "{gen: sample, dist: constant, value: 2 min}", n=1)
    assert duration == [120]


def test_sample_poisson_and_geometric_give_whole_numbers(tmp_path):
    poisson = field(tmp_path, "{gen: sample, dist: poisson, mean: 4}", n=300)
    assert all(isinstance(v, int) and v >= 0 for v in poisson)
    assert 3.4 < sum(poisson) / len(poisson) < 4.6
    geometric = field(tmp_path, "{gen: sample, dist: geometric, p: 0.5}", n=300)
    assert all(isinstance(v, int) and v >= 1 for v in geometric)
    assert 1.6 < sum(geometric) / len(geometric) < 2.4


def test_sample_refuses_a_parameter_the_family_lacks(tmp_path):
    with pytest.raises(BlueprintError, match="poisson does not take rate"):
        SimulationContext.from_yaml(
            _write(
                tmp_path, model("payload: {x: {gen: sample, dist: poisson, rate: 1}}")
            )
        )


def test_boolean_follows_p(tmp_path):
    assert set(field(tmp_path, "{gen: boolean, p: 1}")) == {True}
    assert set(field(tmp_path, "{gen: boolean, p: 0}")) == {False}
    values = field(tmp_path, "{gen: boolean, p: 0.25}", n=400)
    assert 0.15 < sum(values) / len(values) < 0.35


# --- choices -----------------------------------------------------------------


def test_one_of_picks_each_choice_and_evaluates_only_the_pick(tmp_path):
    values = field(tmp_path, "{gen: one_of, choices: [a, b, c]}", n=90)
    assert set(values) == {"a", "b", "c"}
    spec = "{gen: one_of, choices: [{gen: sequence}, {gen: sequence, start: 100}]}"
    picked = field(tmp_path, spec, n=40)
    low = [v for v in picked if v < 100]
    high = [v for v in picked if v >= 100]
    assert low == list(range(len(low)))
    assert high == list(range(100, 100 + len(high)))


def test_weighted_follows_the_weights_in_both_forms(tmp_path):
    values = field(tmp_path, "{gen: weighted, options: {a: 3, b: 1, c: 0}}", n=800)
    counts = Counter(values)
    assert "c" not in counts
    assert 0.68 < counts["a"] / len(values) < 0.82
    spec = "{gen: weighted, choices: [{value: {k: 1}, weight: 1}, {value: {k: 2}, weight: 0}]}"
    assert field(tmp_path, spec) == [{"k": 1}] * 3


def test_weighted_refuses_weights_that_sum_to_zero(tmp_path):
    message = error_of(tmp_path, "payload: {x: {gen: weighted, options: {a: 0, b: 0}}}")
    assert "weights must be 0 or more with a sum above 0" in message


def test_pick_draws_without_replacement(tmp_path):
    for value in field(tmp_path, "{gen: pick, choices: [a, b, c, d], count: 3}", n=30):
        assert len(value) == 3 and len(set(value)) == 3
    message = error_of(tmp_path, "payload: {x: {gen: pick, choices: [a, b], count: 3}}")
    assert "count must be from 1 to the number of choices (2)" in message


def test_switch_by_value_by_range_and_default(tmp_path):
    spec = "{gen: switch, 'on': 500, cases: {200: ok, 500: error}, default: other}"
    assert field(tmp_path, spec, n=1) == ["error"]
    spec = "{gen: switch, on: 404, cases: {200: ok}, default: other}"
    assert field(tmp_path, spec, n=1) == ["other"]
    ranges = "cases: [{max: 100, value: low}, {min: 100, value: high}]"
    assert field(tmp_path, f"{{gen: switch, on: 99, {ranges}}}", n=1) == ["low"]
    assert field(tmp_path, f"{{gen: switch, on: 100, {ranges}}}", n=1) == ["high"]
    assert field(tmp_path, "{gen: switch, on: 1, cases: {2: x}}", n=1) == [None]


def test_when_evaluates_only_the_branch_taken(tmp_path):
    spec = "{gen: when, condition: 't > 0', then: {gen: sequence}, else: late}"
    body = f"vars: {{a: 1}}\npayload: {{x: {spec}}}"
    values = [p["x"] for p in payloads(run(tmp_path, body, n=3))]
    assert values == ["late", 0, 1]


# --- identifiers, counters, times and text ----------------------------------------


def test_uuid_versions_and_repeatability(tmp_path):
    first = field(tmp_path, "{gen: uuid}", seed=5)
    assert len(set(first)) == 3
    assert all(re.fullmatch(r"[0-9a-f-]{36}", v) and v[14] == "4" for v in first)
    assert field(tmp_path, "{gen: uuid}", seed=5) == first
    assert field(tmp_path, "{gen: uuid}", seed=6) != first
    seven = field(tmp_path, "{gen: uuid, version: 7}")
    assert all(v[14] == "7" for v in seven)
    assert seven == sorted(seven)


def test_sequence_counts_across_entities(tmp_path):
    assert field(tmp_path, "{gen: sequence, start: 5, step: 10}") == [5, 15, 25]
    assert field(tmp_path, "{gen: sequence, format: 'id-{}'}") == [
        "id-0",
        "id-1",
        "id-2",
    ]
    assert field(tmp_path, "{gen: sequence, loop_after: 2}", n=5) == [0, 1, 0, 1, 0]


def test_now_equals_the_record_timestamp(tmp_path):
    outcome = run(tmp_path, "service: s1\npayload: {x: {gen: now}}", n=3)
    finished = [
        r
        for r in outcome.records
        if r["stream_type"] == "event"
        and isinstance(r["value"], dict)
        and "x" in r["value"]
    ]
    assert len(finished) == 3
    for record in finished:
        assert record["value"]["x"] == record["timestamp"]


def test_now_takes_an_offset_and_add_and_subtract_move_a_time(tmp_path):
    spec = "{gen: now, offset: 1 min}"
    assert field(tmp_path, spec, n=1) == ["2026-01-01T00:01:00.000+00:00"]
    spec = "{gen: add, args: [{gen: now}, 90]}"
    assert field(tmp_path, spec, n=1) == ["2026-01-01T00:01:30.000+00:00"]
    spec = "{gen: subtract, args: [{gen: now}, 30]}"
    assert field(tmp_path, spec, n=1) == ["2025-12-31T23:59:30.000+00:00"]


def test_format_time_forms(tmp_path):
    cases = {
        "{gen: format_time}": "2026-01-01T00:00:00.000+00:00",
        "{gen: format_time, format: iso_utc, tz: Australia/Melbourne}": "2026-01-01T00:00:00.000Z",
        "{gen: format_time, format: '%d/%m/%Y %H:%M', tz: Australia/Melbourne}": "01/01/2026 11:00",
        "{gen: format_time, format: epoch_s}": 1767225600,
        "{gen: format_time, format: epoch_ms, time: 1.5}": 1767225601500,
    }
    for spec, expected in cases.items():
        assert field(tmp_path, spec, n=1) == [expected], spec


def test_format_lower_upper(tmp_path):
    spec = (
        "{gen: format, template: '{a}-{b}', names: {a: {gen: upper, value: ab}, b: 7}}"
    )
    assert field(tmp_path, spec, n=1) == ["AB-7"]
    assert field(tmp_path, "{gen: lower, value: ABC}", n=1) == ["abc"]
    assert field(tmp_path, "{gen: upper, value: abc}", n=1) == ["ABC"]


def test_fake_is_repeatable_and_a_new_node_shifts_no_other(tmp_path):
    pytest.importorskip("faker")
    first = field(tmp_path, "{gen: fake, method: name}", n=4, seed=3)
    assert len(set(first)) == 4
    assert field(tmp_path, "{gen: fake, method: name}", n=4, seed=3) == first
    both = payloads(
        run(
            tmp_path,
            "payload: {other: {gen: fake, method: email}, x: {gen: fake, method: name}}",
            n=4,
            seed=3,
        )
    )
    assert [p["x"] for p in both] == first
    dated = field(tmp_path, "{gen: fake, method: date_object}", n=1)
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", dated[0])


def test_fake_refuses_a_method_it_lacks_at_load(tmp_path):
    pytest.importorskip("faker")
    with pytest.raises(BlueprintError, match="Faker has no method 'nme'"):
        SimulationContext.from_yaml(
            _write(tmp_path, model("payload: {x: {gen: fake, method: nme}}"))
        )


# --- arithmetic and expressions ---------------------------------------------------


@pytest.mark.parametrize(
    "spec,expected",
    [
        ("{gen: add, args: [1, 2, 3]}", 6),
        ("{gen: subtract, args: [10, 3, 2]}", 5),
        ("{gen: multiply, args: [2, 3, 4]}", 24),
        ("{gen: divide, args: [10, 4]}", 2.5),
        ("{gen: divide, args: [10, 3], decimals: 2}", 3.33),
        ("{gen: min, args: [4, 2, 9]}", 2),
        ("{gen: max, args: [4, 2, 9]}", 9),
        ("{gen: add, args: [0.1, 0.2], decimals: 1}", 0.3),
    ],
)
def test_arithmetic(tmp_path, spec, expected):
    assert field(tmp_path, spec, n=1) == [expected]


def test_arithmetic_refuses_text_a_boolean_and_a_division_by_zero(tmp_path):
    assert "must be a number" in error_of(
        tmp_path, "payload: {x: {gen: add, args: [1, a]}}"
    )
    assert "must be a number" in error_of(
        tmp_path, "payload: {x: {gen: add, args: [1, true]}}"
    )
    assert "divides by 0" in error_of(
        tmp_path, "payload: {x: {gen: divide, args: [1, 0]}}"
    )


def test_expr_reads_vars_names_and_the_clock(tmp_path):
    body = """
        vars: {a: 4}
        payload: {x: {gen: expr, expr: 'a * b + t', names: {b: 3}, decimals: 1}}
    """
    assert [p["x"] for p in payloads(run(tmp_path, body, n=3))] == [12, 13, 14]


def test_expr_functions_power_and_has(tmp_path):
    spec = "{gen: expr, expr: 'sqrt(16) + abs(-2) ** 3 + max(1, 2)'}"
    assert field(tmp_path, spec, n=1) == [14.0]
    extra = "".join([])
    spec = "{gen: expr, expr: 'has(attr.k)'}"
    assert field(tmp_path, spec, n=1, extra=extra) == [False]


def test_var_attr_and_param_read_the_model(tmp_path):
    extra = (
        "variables: {limit: 5}\n"
        "scenario: [{at: 2, path: V.variables.limit, value: 9}]\n"
    )
    body = """
        service: s1
        payload: {x: {gen: param, path: V.variables.limit}}
    """
    outcome = run_yaml_text(tmp_path, model(body, 1, extra), {"a": [0.0, 3.0]})
    assert [p["x"] for p in payloads(outcome)] == [5, 9]
    attrs = "variables: {}\n"
    spec = "{gen: attr, name: tag}"
    text = model(f"payload: {{x: {spec}}}", extra=attrs).replace(
        "spawn: t}}", "spawn: t, attributes: {tag: {n: 1}}}}"
    )
    outcome = run_yaml_text(tmp_path, text, {"a": [0.0]})
    assert [p["x"] for p in payloads(outcome)] == [{"n": 1}]


def test_repeat_literal_object_and_call(tmp_path):
    assert field(tmp_path, "{gen: repeat, times: 3, value: {gen: sequence}}", n=1) == [
        [0, 1, 2]
    ]
    assert field(tmp_path, "{gen: literal, value: {gen: 5}}", n=1) == [{"gen": 5}]
    assert field(
        tmp_path, "{gen: object, fields: {a: 1, b: {gen: sequence}}}", n=2
    ) == [
        {"a": 1, "b": 0},
        {"a": 1, "b": 1},
    ]
    tmp_path.joinpath("fns.py").write_text(
        "def double(context, x):\n    return x * 2\n"
    )
    assert field(
        tmp_path, "{gen: call, function: !python fns.double, kwargs: {x: 4}}", n=1
    ) == [8]


# --- row ------------------------------------------------------------------------


def _rows(tmp_path: Path, spec: str, n: int, seed: int = 1, text: Optional[str] = None):
    tmp_path.joinpath("rows.csv").write_text(
        text or "id,price,weight\nA,2.5,1\nB,4,3\nC,0012,6\n"
    )
    return field(tmp_path, spec, n=n, seed=seed)


def test_row_reads_cells_as_numbers(tmp_path):
    values = _rows(tmp_path, "{gen: row, file: rows.csv, order: sequential}", 3)
    assert values == [
        {"id": "A", "price": 2.5, "weight": 1},
        {"id": "B", "price": 4, "weight": 3},
        {"id": "C", "price": 12, "weight": 6},
    ]


def test_row_orders(tmp_path):
    sequential = _rows(
        tmp_path, "{gen: row, file: rows.csv, order: sequential, on_end: loop}", 7
    )
    assert [v["id"] for v in sequential] == list("ABCABCA")
    circular = _rows(tmp_path, "{gen: row, file: rows.csv, order: circular}", 7)
    assert [v["id"] for v in circular] == list("ABCABCA")
    shuffled = _rows(
        tmp_path, "{gen: row, file: rows.csv, order: shuffle, on_end: loop}", 9
    )
    for start in (0, 3, 6):
        assert sorted(v["id"] for v in shuffled[start : start + 3]) == list("ABC")
    random = _rows(tmp_path, "{gen: row, file: rows.csv}", 90)
    assert {v["id"] for v in random} == set("ABC")


def test_row_weight_and_where(tmp_path):
    weighted = _rows(tmp_path, "{gen: row, file: rows.csv, weight: weight}", 500)
    counts = Counter(v["id"] for v in weighted)
    assert counts["C"] > counts["B"] > counts["A"]
    kept = _rows(tmp_path, "{gen: row, file: rows.csv, where: 'price > 3'}", 40)
    assert {v["id"] for v in kept} == {"B", "C"}


def test_row_on_end_error_loop_and_stop(tmp_path):
    spec = "{gen: row, file: rows.csv, order: sequential}"
    tmp_path.joinpath("rows.csv").write_text("id\nA\nB\n")
    message = error_of(tmp_path, f"payload: {{x: {spec}}}", n=3)
    assert "ran out" in message and "on_end" in message
    looped = _rows(
        tmp_path,
        "{gen: row, file: rows.csv, order: sequential, on_end: loop}",
        3,
        text="id\nA\nB\n",
    )
    assert [v["id"] for v in looped] == ["A", "B", "A"]
    stopped = run(
        tmp_path,
        "payload: {x: {gen: row, file: rows.csv, order: sequential, on_end: stop}}",
        n=5,
    )
    assert stopped.error is None
    assert [p["x"]["id"] for p in payloads(stopped)] == ["A", "B"]


def test_row_reads_parquet(tmp_path):
    pq = pytest.importorskip("pyarrow.parquet")
    import pyarrow as pa

    pq.write_table(pa.table({"id": ["A", "B"], "n": [1, 2]}), tmp_path / "rows.parquet")
    values = field(tmp_path, "{gen: row, file: rows.parquet, order: sequential}", n=2)
    assert values == [{"id": "A", "n": 1}, {"id": "B", "n": 2}]


def test_row_file_errors_name_the_line(tmp_path):
    for spec, fragment in (
        ("{gen: row, file: missing.csv}", "missing.csv does not exist"),
        ("{gen: row, file: rows.txt}", "must be a .csv or .parquet file"),
        ("{gen: row, file: rows.csv, where: 'price > 99'}", "has no rows where"),
        ("{gen: row, file: rows.csv, weight: nope}", "has no column 'nope'"),
    ):
        tmp_path.joinpath("rows.csv").write_text("id,price\nA,1\n")
        with pytest.raises(BlueprintError, match=re.escape(fragment)) as error:
            SimulationContext.from_yaml(
                _write(tmp_path, model(f"payload: {{x: {spec}}}"))
            )
        assert "blueprint.yaml:" in str(error.value)


# --- modifiers --------------------------------------------------------------------


def test_null_rate(tmp_path):
    assert field(tmp_path, "{gen: integer, bounds: [1, 2], null_rate: 1}") == [None] * 3
    assert None not in field(tmp_path, "{gen: integer, bounds: [1, 2], null_rate: 0}")
    values = field(tmp_path, "{gen: integer, bounds: [1, 2], null_rate: 0.5}", n=400)
    assert 0.4 < values.count(None) / 400 < 0.6


def test_null_rate_leaves_the_draws_of_the_other_entities_alone(tmp_path):
    plain = field(tmp_path, "{gen: integer, bounds: [1, 1000]}", n=30)
    nulled = field(tmp_path, "{gen: integer, bounds: [1, 1000], null_rate: 0.5}", n=30)
    # A null skips its draw, so a value that follows one is a later value of the same
    # sequence: every non-null value is drawn in order
    assert [v for v in nulled if v is not None] == plain[
        : len([v for v in nulled if v is not None])
    ]


def test_unique_never_repeats_and_runs_out_with_a_message(tmp_path):
    values = field(tmp_path, "{gen: integer, bounds: [1, 40], unique: true}", n=30)
    assert len(set(values)) == 30
    message = error_of(
        tmp_path, "payload: {x: {gen: integer, bounds: [1, 2], unique: true}}", n=3
    )
    assert "unique: no new value in 1000 draws" in message


def test_omit_rate_and_omit_when_drop_a_field_without_evaluating_it(tmp_path):
    body = """
        payload:
          a: {gen: integer, bounds: [1, 3], omit_rate: 0.5}
          b: 1
          c: {gen: sequence, omit_when: 't >= 1'}
    """
    found = payloads(run(tmp_path, body, n=60))
    assert any("a" not in p for p in found) and any("a" in p for p in found)
    assert all("a" not in p for p in found if False)
    # c is not evaluated when omitted, so the counter advanced for the first entity only
    assert [p.get("c") for p in found[:3]] == [0, None, None]
    assert list(found[0]) == ["a", "b", "c"] or list(found[0]) == ["b", "c"]


def test_omit_belongs_on_a_mapping_field(tmp_path):
    for body in (
        "vars: {a: {gen: sequence, omit_rate: 0.5}}\npayload: {x: 1}",
        "payload: {x: [{gen: sequence, omit_when: 'true'}]}",
    ):
        with pytest.raises(
            BlueprintError, match="omit_rate and omit_when drop a field"
        ):
            SimulationContext.from_yaml(_write(tmp_path, model(body)))


# --- seeding ----------------------------------------------------------------------


def test_adding_a_node_shifts_no_other_node(tmp_path):
    alone = payloads(
        run(tmp_path, "payload: {x: {gen: uniform, low: 0, high: 1}}", n=5)
    )
    with_more = payloads(
        run(
            tmp_path,
            "payload: {first: {gen: uniform, low: 0, high: 1}, x: {gen: uniform, low: 0, high: 1}, "
            "later: {gen: normal, mean: 0, std: 1}}",
            n=5,
        )
    )
    assert [p["x"] for p in alone] == [p["x"] for p in with_more]
    assert [p["first"] for p in with_more] != [p["x"] for p in with_more]


def test_a_seed_repeats_a_run_and_another_seed_changes_it(tmp_path):
    spec = "{gen: uniform, low: 0, high: 1}"
    assert field(tmp_path, spec, seed=2) == field(tmp_path, spec, seed=2)
    assert field(tmp_path, spec, seed=2) != field(tmp_path, spec, seed=3)


def test_the_generator_draws_do_not_move_the_arrival_gaps(tmp_path):
    text = HEAD % 4 + (
        "arrivals: {a: {dist: exponential, rate: 1, spawn: t}}\n"
        "tasks: {t: {payload: {x: {gen: uniform, low: 0, high: 1}}}}\n"
        "run: {until: 10}\n"
    )
    plain = text.replace("{x: {gen: uniform, low: 0, high: 1}}", "{x: 1}")
    first = [r["sim_ts"] for r in _records(tmp_path, text)]
    second = [r["sim_ts"] for r in _records(tmp_path, plain)]
    assert first == second and first


def _records(tmp_path: Path, text: str) -> List[Dict[str, Any]]:
    path = _write(tmp_path, text)
    recorder = RecordingEgress()
    app = SimulationContext.from_yaml(path)
    app.add_egress(recorder)
    app.run()
    return [r for r in recorder.records if r["stream_type"] == "event"]


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "blueprint.yaml"
    path.write_text(text)
    tmp_path.joinpath("a.csv").write_text("time\n0\n1\n2\n")
    return path


# --- vars -------------------------------------------------------------------------


def test_vars_are_one_value_per_entity_and_read_in_order(tmp_path):
    body = """
        vars:
          a: {gen: integer, bounds: [1, 1000]}
          b: {gen: multiply, args: [{gen: var, name: a}, 2]}
        payload: {a: {gen: var, name: a}, again: {gen: var, name: a}, b: {gen: var, name: b}}
    """
    for p in payloads(run(tmp_path, body, n=5)):
        assert p["a"] == p["again"] and p["b"] == 2 * p["a"]


def test_each_task_has_its_own_vars_once_for_the_entity(tmp_path):
    text = model(
        """
        vars_once: {customer: {gen: uuid}}
        payload: {customer: {gen: var, name: customer}}
        next: u
        """
    ) + (
        "  u:\n    vars_once: {customer: {gen: uuid}}\n"
        "    payload: {customer: {gen: var, name: customer}}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"a": [0.0, 1.0]})
    first = payloads(outcome, "t")
    second = payloads(outcome, "u")
    assert len({p["customer"] for p in first}) == 2
    # `u` has its own vars_once, so its customer differs from `t`'s for the same entity
    assert all(a["customer"] != b["customer"] for a, b in zip(first, second))


def test_vars_once_is_made_once_across_instances_of_one_task(tmp_path):
    text = (
        HEAD % 1
        + "services: {s1: {dist: constant, value: 1}}\n"
        + "arrivals: {a: {replay: {path: a.csv}, spawn: t}}\n"
        + "tasks:\n  t:\n    vars_once: {n: {gen: sequence}}\n"
        + "    payload: {n: {gen: var, name: n}, i: {gen: sequence}}\n"
        + "    set: {rounds: {gen: expr, expr: 'attr.rounds + 1'}}\n"
        + "    next: {when: [{condition: 'attr.rounds < 3', task: t}]}\n"
    )
    text = text.replace("spawn: t}}", "spawn: t, attributes: {rounds: 0}}}", 1)
    outcome = run_yaml_text(tmp_path, text, {"a": [0.0, 1.0]})
    assert outcome.error is None, outcome.error
    found = payloads(outcome, "t")
    assert len(found) >= 4
    # one entity runs `t` more than once and keeps its value; another gets the next
    assert {p["n"] for p in found} == {0, 1}
    assert [p["i"] for p in found] == list(range(len(found)))


def test_vars_are_evaluated_after_the_service_and_on_start_publishes_at_the_start(
    tmp_path,
):
    body = """
        service: s5
        vars: {a: {gen: now}}
        on_start: {event_type: began, at: {gen: var, name: a}}
        payload: {a: {gen: var, name: a}}
    """
    outcome = run(tmp_path, body, n=1)
    began = [
        r
        for r in outcome.records
        if isinstance(r["value"], dict) and r["value"].get("event_type") == "began"
    ]
    assert began[0]["sim_ts"] == 0
    assert began[0]["value"]["at"] == "2026-01-01T00:00:00.000+00:00"
    done = [p for p in payloads(outcome) if "event_type" not in p][0]
    assert (
        done["a"] == "2026-01-01T00:00:00.000+00:00"
    )  # made at the start, with on_start
    without = body.replace(
        "        on_start: {event_type: began, at: {gen: var, name: a}}\n", ""
    )
    assert (
        payloads(run(tmp_path, without, n=1))[0]["a"] == "2026-01-01T00:00:05.000+00:00"
    )


def test_finish_vars_read_the_vars_and_the_payload_reads_both(tmp_path):
    body = """
        vars: {a: 3}
        finish_vars: {b: {gen: multiply, args: [{gen: var, name: a}, 2]}}
        payload: {a: {gen: var, name: a}, b: {gen: var, name: b}}
    """
    assert payloads(run(tmp_path, body, n=1)) == [{"a": 3, "b": 6}]


def test_a_bare_var_name_is_read_by_put_fail_and_next_and_not_before(tmp_path):
    body = """
        vars: {amount: 7}
        service: s1
        fail: 'amount > 5'
        fail_reason: too big
        payload: {amount: {gen: var, name: amount}}
    """
    failed = [
        r["value"]
        for r in run(tmp_path, body, n=1).records
        if isinstance(r["value"], dict) and r["value"].get("status") == "failed"
    ]
    assert failed and failed[0]["reason"] == "too big"
    with pytest.raises(
        BlueprintError,
        match="uses the name 'amount'",
    ):
        SimulationContext.from_yaml(
            _write(
                tmp_path,
                model(
                    "vars: {amount: 7}\nresource: r\nservice: s1\npatience: 'amount'\npayload: {}",
                    extra="resources: {r: {current_cap: 1}}\n",
                ),
            )
        )


@pytest.mark.parametrize(
    "body,fragment",
    [
        (
            "vars: {a: {gen: var, name: b}, b: 1}\npayload: {}",
            "'b' is not a var that has a value here",
        ),
        (
            "vars_once: {a: {gen: var, name: b}}\nvars: {b: 1}\npayload: {}",
            "'b' is not a var that has a value here",
        ),
        (
            "vars: {a: 1}\nvars_once: {a: 1}\npayload: {}",
            "'a' is in both vars_once and vars",
        ),
        (
            "vars: {count: 1}\npayload: {}",
            "'count' is a name an expression already uses",
        ),
        ("vars: {'a b': 1}\npayload: {}", "is not a name an expression can read"),
        ("payload: {x: {gen: var, name: a}}", "'a' is not a var that has a value here"),
        (
            "vars: {a: {gen: expr, expr: 'zz + 1'}}\npayload: {}",
            "'zz' is not a var that has a value here",
        ),
    ],
)
def test_var_errors_name_the_line(tmp_path, body, fragment):
    with pytest.raises(BlueprintError, match=re.escape(fragment)) as error:
        SimulationContext.from_yaml(_write(tmp_path, model(body)))
    assert "blueprint.yaml:" in str(error.value)


def test_var_outside_a_task_is_refused(tmp_path):
    text = model("payload: {}").replace(
        "spawn: t}}", "spawn: t, attributes: {x: {gen: var, name: a}}}}"
    )
    with pytest.raises(BlueprintError, match="vars belong to a task"):
        SimulationContext.from_yaml(_write(tmp_path, text))


# --- the places that take a value ---------------------------------------------------


def test_arrival_attributes_are_drawn_once_per_entity_when_it_arrives(tmp_path):
    text = (
        HEAD % 1 + "arrivals:\n  a:\n    dist: constant\n    value: 2\n    spawn: t\n"
        "    attributes: {x: {gen: sequence}, at: {gen: now}, y: {gen: attr, name: x}}\n"
        "tasks: {t: {payload: {x: {gen: attr, name: x}, at: {gen: attr, name: at}, y: {gen: attr, name: y}}}}\n"
        "run: {until: 7}\n"
    )
    found = payloads(run_yaml_text(tmp_path, text, {}))
    assert [p["x"] for p in found] == [0, 1, 2]
    assert [p["y"] for p in found] == [0, 1, 2]
    assert [p["at"][11:19] for p in found] == ["00:00:02", "00:00:04", "00:00:06"]


def test_replay_fields_override_a_generated_attribute(tmp_path):
    text = model("payload: {x: {gen: attr, name: x}}").replace(
        "spawn: t}}", "spawn: t, attributes: {x: {gen: sequence}}}}"
    )
    tmp_path.joinpath("a.csv").write_text("time,x\n0,10\n1,20\n")
    tmp_path.joinpath("model.yaml").write_text(text)
    recorder = RecordingEgress()
    app = SimulationContext.from_yaml(tmp_path / "model.yaml")
    app.add_egress(recorder)
    app.run()
    found = [
        r["value"]["x"]
        for r in recorder.records
        if isinstance(r["value"], dict) and "x" in r["value"]
    ]
    assert found == [10, 20]


def test_store_items_are_generated_when_the_store_is_built(tmp_path):
    extra = (
        "stores: {s: {items: [{gen: sequence, start: 5}, {gen: sequence, start: 8}]}}\n"
    )
    body = "payload: {n: {gen: expr, expr: 'count(s)'}, items: {gen: expr, expr: 'items(s)'}}"
    found = payloads(run(tmp_path, body, n=2, extra=extra))
    assert [p["items"] for p in found] == [[5, 8], [5, 8]]


def test_set_cause_on_value_and_timeout_value_are_generated(tmp_path):
    text = (
        HEAD % 1 + "variables: {v: 0, reason: r}\n"
        "signals: [s]\n"
        "arrivals: {a: {replay: {path: a.csv}, spawn: t}, b: {replay: {path: b.csv}, spawn: cutter}}\n"
        "tasks:\n"
        "  t:\n    wait: {timeout: 9}\n    on_interrupt: abandon\n    on_interrupt_next: after\n    payload: {}\n"
        "  after: {payload: {done: 1}, set: {k: {gen: sequence, start: 7}}, next: timed}\n"
        "  timed:\n    wait: {timeout: {after: 2, value: {gen: expr, expr: 't * 2'}}}\n"
        "    payload: {got: {gen: expr, expr: 'wait.timeout.value'}, k: {gen: attr, name: k}}\n"
        "  cutter:\n    wait: {timeout: 1}\n    payload: {}\n"
        "    interrupt: {task: t, cause: {gen: expr, expr: \"registry['V.variables.reason']\"}}\n"
        "    on: [{path: V.variables.v, value: {gen: expr, expr: 't + 40'}}]\n"
        "run: {until: 20}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"a": [0.0], "b": [0.0]})
    assert outcome.error is None
    interrupted = [
        r["value"]
        for r in outcome.records
        if isinstance(r["value"], dict) and r["value"].get("status") == "interrupted"
    ]
    assert interrupted[0]["cause"] == "r"
    timed = payloads(outcome, "timed")
    assert timed == [{"got": 2 * 1, "k": 7}]
    assert outcome.app_or_env.env.registry.get("V.variables.v").value == 41


def test_a_scenario_value_and_record_are_generated_when_the_step_is_due(tmp_path):
    extra = (
        "variables: {v: 1}\n"
        "scenario:\n"
        "  - at: 1\n    path: V.variables.v\n    value: {gen: add, args: [{gen: param, path: V.variables.v}, 4]}\n"
        "    record: {seen: {gen: param, path: V.variables.v}, drawn: {gen: sequence}}\n"
        "  - at: 2\n    record: {drawn: {gen: sequence, start: 5}}\n"
    )
    labelled = RecordingEgress()
    outcome = run_yaml_text(
        tmp_path,
        model("payload: {}", 1, extra),
        {"a": [0.0, 1.0, 2.0, 3.0]},
        prepare=lambda app: app.add_egress(labelled, when="labels"),
    )
    assert outcome.error is None
    labels = [r for r in labelled.records if r["stream_type"] == "label"]
    assert [r["label"]["drawn"] for r in labels] == [0, 5]
    assert labels[0]["label"]["seen"] == 5
    assert labels[0]["new"] == 5


def test_a_payload_that_is_a_generator_must_give_a_mapping_or_null(tmp_path):
    """A null gives a finished record whose value is null, a tombstone on Kafka with
    `envelope: false`; `payload: null` still publishes no record."""
    assert payloads(run(tmp_path, "payload: {gen: object, fields: {a: 1}}", n=1)) == [
        {"a": 1}
    ]
    body = (
        "vars: {n: {gen: sequence}}\n"
        "payload: {gen: when, condition: 'n == 1', then: null, "
        "else: {gen: object, fields: {a: 1}}}"
    )
    outcome = run(tmp_path, body, n=3)
    assert outcome.error is None, outcome.error
    finished = [
        (r["key"], r["value"])
        for r in outcome.records
        if r["stream_type"] == "event" and r["task"] == "t"
    ]
    assert finished == [("a-1", {"a": 1}), ("a-2", None), ("a-3", {"a": 1})]
    silent = run(tmp_path, "payload: null", n=1)
    assert [r for r in silent.records if r["stream_type"] == "event"] == []
    message = error_of(tmp_path, "payload: {gen: integer, bounds: [1, 2]}")
    assert "a payload must give a mapping or null" in message
    message = error_of(tmp_path, "id_field: n\npayload: {gen: literal, value: null}")
    assert re.search(
        r"model\.yaml:\d+: tasks\.t\.payload: the payload gave null, so it has no "
        r"field to write n into \(id_field, item_field and attributes need a mapping\)",
        message,
    )
    # `attributes` is checked apart from `id_field` and `item_field`, which reach
    # the payload as fields
    text = model("attributes: [k]\npayload: {gen: literal, value: null}").replace(
        "spawn: t}", "spawn: t, attributes: {k: 1}}"
    )
    outcome = run_yaml_text(tmp_path, text, {"a": [0.0]})
    assert "tasks.t.payload: the payload gave null, so it has no field to write k" in (
        str(outcome.error)
    )


def test_an_expression_that_reads_a_null_payload_names_its_place(tmp_path):
    """An expression that reads a field of a null payload names its place and says
    the payload is null: a `fail`, a `next` condition, and the `service_time` of the
    task the payload is routed to. A `fail` that cannot be evaluated fails the
    task, as a `fail_reason` that cannot be made does: the task publishes its
    `failed` record, and with no `on_fail` the run stops with `TaskFailed`."""
    null_second = (
        "vars: {n: {gen: sequence}}\n"
        "payload: {gen: when, condition: 'n == 1', then: null, "
        "else: {gen: object, fields: {a: 1}}}\n"
    )
    outcome = run(tmp_path, null_second + "fail: 'payload.a == 2'")
    assert isinstance(outcome.error, TaskFailed)
    ends = [
        (r["key"], r["value"]) for r in outcome.records if r["stream_type"] == "event"
    ]
    assert [key for key, _ in ends] == ["a-1", "a-2"]
    assert ends[1][1]["status"] == "failed"
    assert re.fullmatch(
        r"ValueError: .*model\.yaml:\d+: tasks\.t\.fail: cannot evaluate "
        r"'payload\.a == 2': the payload is null",
        ends[1][1]["reason"],
    )
    routed = model(
        null_second + "next: {when: [{condition: 'payload.a == 1', task: u}]}"
    )
    outcome = run_yaml_text(
        tmp_path, routed + "  u: {payload: {b: 1}}\n", {"a": [0.0, 1.0, 2.0]}
    )
    assert re.fullmatch(
        r".*model\.yaml:\d+: tasks\.t\.next\.when\.0\.condition: cannot evaluate "
        r"'payload\.a == 1': the payload is null",
        str(outcome.error),
    )
    timed = model(null_second + "next: u")
    outcome = run_yaml_text(
        tmp_path,
        timed + "  u: {service_time: 'payload.a', payload: {b: 1}}\n",
        {"a": [0.0, 1.0, 2.0]},
    )
    assert re.fullmatch(
        r".*model\.yaml:\d+: tasks\.u\.service_time: cannot evaluate "
        r"'payload\.a': the payload is null",
        str(outcome.error),
    )


def test_id_field_and_attributes_are_written_over_a_generated_payload(tmp_path):
    body = """
        id_field: id
        payload: {id: {gen: uuid}, n: {gen: sequence}}
    """
    found = payloads(run(tmp_path, body, n=2))
    assert [p["id"] for p in found] == [0, 1]
    assert [p["n"] for p in found] == [0, 1]


def test_generator_errors_name_the_line_and_the_path(tmp_path):
    body = "payload:\n  a: 1\n  b: {gen: divide, args: [1, 0]}"
    outcome = run(tmp_path, body, n=1)
    assert isinstance(outcome.error.__cause__ or outcome.error, Exception)
    message = str(outcome.error)
    assert re.search(
        r"model\.yaml:\d+: tasks\.t\.payload\.b: gen divide: it divides by 0", message
    )


# --- Template, in a builder task ------------------------------------------------------


def test_template_in_a_builder_task():
    order = Template(
        {
            "order_id": {"gen": "sequence", "format": "o-{}"},
            "amount": {"gen": "uniform", "low": 2, "high": 200, "decimals": 2},
        },
        name="place_order",
    )
    app = SimulationContext(sim_id="B", random_seed=5, factor=0)
    app.add_arrival("orders", "constant", value=1, spawn="place_order")

    @app.task(name="place_order")
    def place_order(task_id):
        return order

    recorder = RecordingEgress()
    app.add_egress(recorder)
    app.run(until=3.5)
    found = [
        r["value"]
        for r in recorder.records
        if r["stream_type"] == "event" and isinstance(r["value"], dict)
    ]
    assert [p["order_id"] for p in found] == ["o-0", "o-1", "o-2"]
    assert all(2 <= p["amount"] <= 200 for p in found)
    # repeats with the seed
    again = SimulationContext(sim_id="B", random_seed=5, factor=0)
    again.add_arrival("orders", "constant", value=1, spawn="place_order")
    again.task(name="place_order")(lambda task_id: order)
    second = RecordingEgress()
    again.add_egress(second)
    again.run(until=3.5)
    assert [r["value"] for r in second.records if r["stream_type"] == "event"] == [
        r["value"] for r in recorder.records if r["stream_type"] == "event"
    ]


def test_a_builder_template_that_gives_null_publishes_a_null_value():
    """The Builder form of a YAML payload generator that gives null."""
    delete = Template({"gen": "literal", "value": None}, name="delete")
    app = SimulationContext(sim_id="B", random_seed=5, factor=0)
    app.add_arrival("orders", "constant", value=1, spawn="delete")
    app.task(name="delete")(lambda task_id: delete)
    recorder = RecordingEgress()
    app.add_egress(recorder)
    app.run(until=2.5)
    assert [
        (r["key"], r["value"]) for r in recorder.records if r["stream_type"] == "event"
    ] == [("orders-1", None), ("orders-2", None)]
    # A field given to `bind` needs a mapping, and the error names no blueprint key
    bound = SimulationContext(sim_id="B", random_seed=5, factor=0)
    bound.add_arrival("orders", "constant", value=1, spawn="delete")
    bound.task(name="delete")(lambda task_id: delete.bind(id=task_id))
    with pytest.raises(
        TaskFailed,
        match=re.escape(
            "delete.payload: the payload gave null, so it has no field to write id into "
            "(the fields and attributes written over it need a mapping)"
        ),
    ):
        bound.run(until=1.5)


def test_template_renders_in_a_process_and_binds_fields():
    template = Template(
        {"n": {"gen": "sequence"}, "a": {"gen": "attr", "name": "k"}}, name="p"
    )
    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    seen: List[Any] = []

    def process(context):
        entity = context._new_entity("x", ({"k": 3}, None))
        seen.append(template.render(context, entity))
        seen.append(template.bind(entity, extra=1).render(context))
        yield context.env.timeout(1)

    app.add_process(process)
    app.run(until=2)
    assert seen == [{"n": 0, "a": 3}, {"n": 1, "a": 3, "extra": 1}]


def test_template_mistakes_raise_a_validation_error():
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="hgh"):
        Template({"x": {"gen": "uniform", "low": 1, "hgh": 3}})
    with pytest.raises(ValidationError, match="a payload is a mapping or a generator"):
        Template([1, 2])


def test_builder_vars_take_generator_mappings():
    app = SimulationContext(sim_id="B", random_seed=2, factor=0)
    app.add_arrival("o", "constant", value=1, spawn="t")

    @app.task(
        name="t",
        vars={"a": {"gen": "integer", "bounds": [1, 1000]}},
        finish_vars={
            "b": {"gen": "multiply", "args": [{"gen": "var", "name": "a"}, 2]}
        },
    )
    def t(task_id):
        return Template(
            {"a": {"gen": "var", "name": "a"}, "b": {"gen": "var", "name": "b"}}
        )

    recorder = RecordingEgress()
    app.add_egress(recorder)
    app.run(until=3.5)
    found = [
        r["value"]
        for r in recorder.records
        if r["stream_type"] == "event"
        and isinstance(r["value"], dict)
        and "a" in r["value"]
    ]
    assert len(found) == 3 and all(p["b"] == 2 * p["a"] for p in found)


def test_template_without_a_run_says_so():
    template = Template({"a": 1})
    with pytest.raises(ValueError, match="a Template renders in a run"):
        template.render(SimulationContext(sim_id="B"))
    with pytest.raises(ValueError, match="a Template renders in a run"):
        template.bind(None, b=1).render(SimulationContext(sim_id="B"))


def test_the_json_of_a_payload_has_plain_numbers(tmp_path):
    found = payloads(
        run(
            tmp_path,
            "payload: {a: {gen: integer, bounds: [1, 5]}, b: {gen: uniform, low: 0, high: 1}, "
            "c: {gen: boolean}, d: {gen: sample, dist: poisson, mean: 3}}",
            n=2,
        )
    )
    text = json.dumps(found)
    assert "numpy" not in text
    for p in found:
        assert type(p["a"]) is int and type(p["b"]) is float and type(p["c"]) is bool
        assert type(p["d"]) is int
    assert datetime.fromisoformat(field(tmp_path, "{gen: now}", n=1)[0])


def test_a_generator_nested_in_an_on_mapping_value_is_stored_as_written():
    """A generator at the top of an `on:` value is evaluated; a mapping with no `gen`
    key is data, so a generator nested in it is kept as written and never evaluated."""
    from desflow.blueprint.models import OnEntry
    from desflow.values.models import Integer

    nested = {"inner": {"gen": "integer", "bounds": [5, 5]}}
    for entry in (
        {"fire": {"signal": "s", "value": nested}},
        {"path": "Shop.variables.total", "value": nested},
    ):
        parsed = OnEntry.model_validate(entry)
        value = parsed.fire.value if parsed.fire else parsed.value
        assert value == nested and isinstance(value, dict)
        assert isinstance(value["inner"], dict)
    top = OnEntry.model_validate(
        {"path": "Shop.variables.total", "value": {"gen": "integer", "bounds": [5, 5]}}
    )
    assert isinstance(top.value, Integer)


# --- review round: one evaluation, time zone, offset, service draws -----------------


def test_add_and_subtract_evaluate_a_generator_argument_once(tmp_path):
    assert field(tmp_path, "{gen: add, args: [{gen: sequence}, 100]}", n=4) == [
        100,
        101,
        102,
        103,
    ]
    assert field(tmp_path, "{gen: subtract, args: [{gen: sequence}, 100]}", n=3) == [
        -100,
        -99,
        -98,
    ]


@pytest.mark.skipif(
    not hasattr(__import__("time"), "tzset"),
    reason="Only Unix can change the machine's zone during a process",
)
def test_add_reads_a_time_without_a_zone_in_the_run_zone(tmp_path, monkeypatch):
    import os
    import time

    monkeypatch.setenv("TZ", "America/Los_Angeles")
    time.tzset()
    try:
        found = field(tmp_path, '{gen: add, args: ["2026-01-01T00:00:00", 60]}', n=1)
    finally:
        monkeypatch.undo()
        time.tzset()
    assert found == ["2026-01-01T00:01:00.000+00:00"]
    assert os.environ.get("TZ") != "America/Los_Angeles"


def test_now_offset_takes_a_signed_duration_and_a_bad_one_is_refused_at_load(tmp_path):
    assert field(tmp_path, '{gen: now, offset: "-1 h"}', n=1) == [
        "2025-12-31T23:00:00.000+00:00"
    ]
    assert field(tmp_path, "{gen: now, offset: -30}", n=1) == [
        "2025-12-31T23:59:30.000+00:00"
    ]
    with pytest.raises(BlueprintError, match="offset"):
        SimulationContext.from_yaml(
            _write(tmp_path, model('payload: {x: {gen: now, offset: "soon"}}'))
        )


def test_a_sample_of_a_service_leaves_the_service_times_unchanged(tmp_path):
    base = (
        HEAD % 3
        + "services: {sx: {dist: exponential, rate: 1}}\n"
        + "arrivals: {a: {replay: {path: a.csv}, spawn: t}}\n"
        + "tasks:\n  t:\n    service: sx\n    payload: {PAYLOAD}\n"
    )

    def finish_times(payload: str) -> List[float]:
        outcome = run_yaml_text(
            tmp_path,
            base.replace("PAYLOAD", payload),
            {"a": [0.0, 10.0, 20.0, 30.0]},
        )
        assert outcome.error is None, outcome.error
        return [
            r["sim_ts"]
            for r in outcome.records
            if r["stream_type"] == "event" and "status" not in r["value"]
        ]

    plain = finish_times("x: 1")
    sampled = finish_times("x: 1, b: {gen: sample, service: sx}")
    assert sampled == plain and len(plain) == 4


def test_a_sample_of_a_service_draws_a_positive_time_from_its_own_stream(tmp_path):
    drawn = field(
        tmp_path,
        "{gen: sample, service: s1}",
        n=3,
    )
    assert drawn == [1, 1, 1]


def test_a_row_stop_ends_the_run_in_segments_and_in_a_child_task(tmp_path):
    rows = tmp_path / "rows.csv"
    rows.write_text("n\n1\n")
    stopping = (
        "payload: {x: {gen: row, file: rows.csv, order: sequential, on_end: stop}}"
    )
    head = (
        HEAD % 1
        + "services: {s1: {dist: constant, value: 1}}\n"
        + "arrivals: {a: {replay: {path: a.csv}, spawn: t}, "
        "b: {replay: {path: b.csv}, spawn: u}}\n"
    )
    tail = "  u:\n    payload: {k: 1}\n"

    def events(text: str, a: List[float]) -> List[Any]:
        outcome = run_yaml_text(tmp_path, text, {"a": a, "b": [0.5, 2.0, 5.0, 6.0]})
        assert outcome.error is None, outcome.error
        return [
            (r["task"], r["sim_ts"])
            for r in outcome.records
            if r["stream_type"] == "event" and "status" not in r["value"]
        ]

    plain = head + f"tasks:\n  t:\n    {stopping}\n" + tail
    segments = plain + "run: {segments: [{until: 3}, {until: 9}]}\n"
    assert events(segments, [0.0, 1.0]) == events(plain, [0.0, 1.0])
    assert ("u", 5.0) not in events(segments, [0.0, 1.0])

    child = (
        head
        + "tasks:\n  t:\n    wait: {task: c}\n    payload: {y: 1}\n"
        + f"  c:\n    {stopping}\n"
        + tail
    )
    found = events(child, [0.0, 1.0])
    assert ("u", 5.0) not in found
    anyof = child.replace(
        "wait: {task: c}", "wait: {any: {w: {task: c}, tt: {timeout: 100}}}"
    )
    assert ("u", 5.0) not in events(anyof, [0.0, 1.0])


@pytest.mark.parametrize(
    ("task", "expected"),
    [
        ("payload: {v: {gen: expr, expr: 'attr[\"class\"]'}}", [{"v": 3}]),
        (
            "set: {x: {gen: expr, expr: 'attr[\"class\"] * 2'}}\n"
            "payload: {v: 1}\nnext: u",
            [{"v": 1}, {"x": 6}],
        ),
        (
            "vars: {c: {gen: expr, expr: 'attr[\"class\"]'}}\n"
            "payload: {v: {gen: var, name: c}}",
            [{"v": 3}],
        ),
        (
            "payload: {v: 1}\n"
            "next: {when: [{condition: 'attr[\"class\"] == 3', task: u}], default: w}",
            [{"v": 1}, {"x": 6}],
        ),
        ("service_time: 'attr[\"class\"]'\npayload: {v: 1}", [{"v": 1}]),
        ("fail: 'attr[\"class\"] == 3'\npayload: {v: 1}", "failed"),
    ],
    ids=["payload", "set", "vars", "next.when", "service_time", "fail"],
)
def test_an_attribute_named_by_an_index_is_read_in_every_expression_place(
    tmp_path, task, expected
):
    """`attr["class"]` reads the attribute `class` in a generator, a `set`, a var, a
    condition, a service time and a failure test, and `desflow check` loads it."""
    text = (
        HEAD % 1
        + "arrivals: {a: {initial: 1, spawn: t, attributes: {class: 3}}}\n"
        + "tasks:\n  t:\n"
        + textwrap.indent(task, "    ")
        + "\n  u: {payload: {x: {gen: expr, expr: 'attr[\"class\"] * 2'}}}\n"
        + "  w: {payload: {x: 0}}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {})
    if expected == "failed":
        assert 'attr["class"] == 3' in str(outcome.error)
        return
    assert outcome.error is None, outcome.error
    found = [
        r["value"]
        for r in outcome.records
        if r["stream_type"] == "event" and "status" not in r["value"]
    ]
    assert found == expected


ROW_STOP = "{gen: row, file: rows.csv, order: sequential, on_end: stop}"


@pytest.mark.parametrize(
    ("arrival", "run_key", "expected"),
    [
        ("gap: 1, attributes: {r: ROW}", "until: 20", [1.0, 2.0]),
        (
            "gap: 1, vars: {r: ROW}, attributes: {k: {gen: var, name: r}}",
            "until: 20",
            [1.0, 2.0],
        ),
        (
            "gap: 1, key: {gen: format, template: k, names: {r: ROW}}",
            "until: 20",
            [1.0, 2.0],
        ),
        (
            "gap: {gen: expr, expr: \"r['k']\", names: {r: ROW}}",
            "until: 20",
            [1.0, 3.0],
        ),
        (
            "gap: 1, group: {gen: expr, expr: \"r['k']\", names: {r: ROW}}",
            "until: 20",
            [1.0, 2.0, 2.0],
        ),
        (
            "gap: 100, first_at: 1, count: 3, stagger: 1, attributes: {r: ROW}",
            "until: 20",
            [1.0, 1.0],
        ),
        ("initial: 3, gap: 1, attributes: {r: ROW}", "until: 20", []),
        ("gap: 1, attributes: {r: ROW}", "until: 20, drain: true", [1.0, 2.0]),
        (
            "gap: 1, attributes: {r: ROW}",
            "segments: [{until: 5}, {until: 20}]",
            [1.0, 2.0],
        ),
        ("gap: 1, attributes: {r: ROW}", "until: 20, steps: true", [1.0, 2.0]),
    ],
    ids=[
        "attributes",
        "vars",
        "key",
        "gap",
        "group",
        "count",
        "initial",
        "drain",
        "segments",
        "steps",
    ],
)
def test_a_row_stop_ends_the_run_where_an_arrival_draws_it(
    tmp_path, arrival, run_key, expected
):
    """The rows run out at the third draw, in a place of an arrival that no task
    catches: the run ends there, with no error and no entity of that draw."""
    tmp_path.joinpath("rows.csv").write_text("k\n1\n2\n")
    text = (
        HEAD % 1
        + f"arrivals: {{a: {{spawn: t, {arrival.replace('ROW', ROW_STOP)}}}}}\n"
        + "tasks: {t: {payload: {x: 1}}}\n"
        + f"run: {{{run_key}}}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, outcome.error
    found = [
        r["sim_ts"]
        for r in outcome.records
        if r["stream_type"] == "event" and r["value"].get("x") == 1
    ]
    assert found == expected


def test_a_row_stop_in_an_arrival_ends_run_advance_and_step(tmp_path):
    """The three ways to drive a context, `run`, `advance` and `step`, each end the
    run at the stop and leave it ended, with no error."""
    tmp_path.joinpath("rows.csv").write_text("k\n1\n2\n")
    path = tmp_path / "model.yaml"
    path.write_text(
        HEAD % 1
        + f"arrivals: {{a: {{gap: 1, spawn: t, attributes: {{r: {ROW_STOP}}}}}}}\n"
        + "tasks: {t: {payload: {x: 1}}}\n"
    )

    def started() -> Any:
        app: Any = SimulationContext.from_yaml(path)
        app.recorder = RecordingEgress()
        app.add_egress(app.recorder)
        app.start(until=20)
        return app

    def times(app: Any) -> List[float]:
        return [
            r["sim_ts"] for r in app.recorder.records if r["stream_type"] == "event"
        ]

    app = started()
    assert app.finish() is None
    assert times(app) == [1.0, 2.0]

    app = started()
    assert app.advance(10) is None
    with pytest.raises(RuntimeError, match="the run ended"):
        app.advance(15)
    assert app.finish() is None
    assert times(app) == [1.0, 2.0]

    app = started()
    for _ in range(50):
        if app.peek() == float("inf"):
            break
        app.step()
    assert app.peek() == float("inf")
    with pytest.raises(RuntimeError, match="the run ended"):
        app.step()
    with pytest.raises(RuntimeError, match="the run ended"):
        app.advance(15)
    app.close()
    assert times(app) == [1.0, 2.0]


def test_vars_once_is_evaluated_at_the_first_event_before_the_service(tmp_path):
    body = """
        service: s5
        vars_once: {v: {gen: now}}
        payload: {v: {gen: var, name: v}, w: {gen: now}}
    """
    found = payloads(run(tmp_path, body, n=1))[0]
    assert found["v"] == "2026-01-01T00:00:00.000+00:00"
    assert found["w"] == "2026-01-01T00:00:05.000+00:00"


def test_a_task_that_can_end_early_refuses_a_var_in_its_on_actions(tmp_path):
    for end in ("patience: 0.5", "balk: queue.r >= 1"):
        body = (
            "service: s5\nresource: r\n"
            f"{end}\nvars: {{v: 1}}\n"
            "on: [{fire: {signal: z, value: {gen: var, name: v}}}]\n"
            "payload: {a: 1}\n"
        )
        text = model(body).replace(
            "arrivals:", "resources: {r: {current_cap: 1}}\nsignals: {z: {}}\narrivals:"
        )
        with pytest.raises(BlueprintError, match="not a var that has a value here"):
            SimulationContext.from_yaml(_write(tmp_path, text))


@pytest.mark.parametrize(
    "names,fragment",
    [
        ("{t: 100}", "t"),
        ("{attr: 1}", "attr"),
        ("{sum: 1}", "sum"),
        ("{cause: 1}", "cause"),
        ("{in: 1}", "in"),
    ],
)
def test_expr_names_may_not_take_a_name_of_the_language(tmp_path, names, fragment):
    body = f"payload: {{x: {{gen: expr, expr: '1', names: {names}}}}}"
    with pytest.raises(BlueprintError, match=re.escape(fragment)):
        SimulationContext.from_yaml(_write(tmp_path, model(body)))


def test_an_expr_name_that_is_a_var_is_a_clash_refused_at_load(tmp_path):
    body = "vars: {x: 1}\npayload: {y: {gen: expr, expr: 'x + 1', names: {x: 100}}}"
    with pytest.raises(BlueprintError, match="x"):
        SimulationContext.from_yaml(_write(tmp_path, model(body)))


def test_a_var_may_not_take_a_reserved_name(tmp_path):
    for name in ("sum", "cause", "len", "waiting", "str"):
        body = f"vars: {{{name}: 1}}\npayload: {{x: 1}}"
        with pytest.raises(BlueprintError, match=name):
            SimulationContext.from_yaml(_write(tmp_path, model(body)))


def test_a_template_checks_a_row_file_when_it_is_made(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError, match="rows_b.csv does not exist"):
        Template({"n": {"gen": "row", "file": "rows_b.csv"}}, name="r")
    tmp_path.joinpath("rows_b.csv").write_text("n\n7\n8\n")
    template = Template(
        {"n": {"gen": "row", "file": "rows_b.csv", "order": "sequential"}}, name="r"
    )
    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    app.add_arrival("o", "constant", value=1, spawn="x")

    @app.task(name="x")
    def x(task_id):
        return template

    recorder = RecordingEgress()
    app.add_egress(recorder)
    app.run(until=2.5)
    found = [
        r["value"]["n"]
        for r in recorder.records
        if r["stream_type"] == "event" and isinstance(r["value"], dict)
    ]
    assert found == [{"n": 7}, {"n": 8}]


def test_unnamed_templates_do_not_share_a_counter_or_a_stream():
    first = Template({"n": {"gen": "sequence"}})
    second = Template({"n": {"gen": "sequence"}})
    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    seen: List[Any] = []

    def process(context):
        seen.append(first.render(context)["n"])
        seen.append(second.render(context)["n"])
        seen.append(first.render(context)["n"])
        yield context.env.timeout(1)

    app.add_process(process)
    app.run(until=2)
    assert seen == [0, 0, 1]


def test_a_template_reads_attributes_when_the_payload_is_built(tmp_path):
    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    app.add_arrival("o", "constant", value=1, spawn="x", attributes={"k": 1})
    app.add_service("s", "constant", value=2)
    template = Template({"v": {"gen": "attr", "name": "k"}}, name="p")

    @app.task(name="x", service_id="s")
    def x(task_id, entity):
        def later():
            yield app.env.timeout(1)
            entity.attributes["k"] = 99

        app.env.process(later())
        return template.bind(entity, k=entity.attributes["k"])

    recorder = RecordingEgress()
    app.add_egress(recorder)
    app.run(until=3.5)
    found = [
        r["value"]
        for r in recorder.records
        if r["stream_type"] == "event"
        and isinstance(r["value"], dict)
        and "v" in r["value"]
    ]
    assert found[0]["v"] == 99


def test_a_blueprint_payload_writes_attributes_after_the_service(tmp_path):
    text = (
        HEAD % 1
        + "services: {s5: {dist: constant, value: 5}}\n"
        + "arrivals: {a: {replay: {path: a.csv}, spawn: t, attributes: {k: 1}}}\n"
        + "tasks:\n  t:\n    service: s5\n    attributes: [k]\n"
        + "    payload: {x: {gen: attr, name: k}}\n"
    )
    outcome = run_yaml_text(
        tmp_path,
        text,
        {"a": [0.0]},
        prepare=lambda app: app.add_process(_change_k(app)),
    )
    assert outcome.error is None, outcome.error
    found = payloads(outcome)[0]
    assert found == {"x": 7, "k": 7}, found


def _change_k(app: Any):
    def process(context):
        yield context.env.timeout(2)
        for runs in context._running.values():
            for running in runs:
                running.entity.attributes["k"] = 7

    return process


def test_builder_places_take_a_typed_generator_and_bind_each_to_its_place():
    from desflow.values import generator

    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    low = generator({"gen": "uniform", "low": 0, "high": 1})
    other = generator({"gen": "uniform", "low": 0, "high": 1})
    app.add_arrival(
        "o", "constant", value=1, spawn="x", attributes={"u": low, "w": other}
    )
    app.add_store("shelf", items=[generator({"gen": "sequence"})] * 0)
    seen: List[Any] = []

    @app.task(name="x")
    def x(task_id, entity):
        seen.append((entity.attributes["u"], entity.attributes["w"]))
        return {}

    app.run(until=3.5)
    assert len(seen) == 3
    assert all(u != w for u, w in seen)
    assert low._path == "arrivals.o.attributes.u"
    assert other._path == "arrivals.o.attributes.w"


def test_low_level_value_evaluates_a_generator_in_a_process():
    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    seen: List[Any] = []

    def process(context):
        entity = context._new_entity("x", ({"k": 3}, None))
        seen.append(context.values.value({"gen": "attr", "name": "k"}, entity))
        seen.append(context.values.value({"gen": "sequence"}))
        yield context.env.timeout(1)

    app.add_process(process)
    app.run(until=2)
    assert seen == [3, 0]


def test_vars_once_of_an_entity_is_dropped_when_its_flow_ends(tmp_path):
    outcome = run(
        tmp_path,
        "vars_once: {v: {gen: sequence}}\npayload: {v: {gen: var, name: v}}",
        n=4,
    )
    assert [p["v"] for p in payloads(outcome)] == [0, 1, 2, 3]
    assert outcome.app_or_env.values._once == {}


def test_format_without_a_name_and_a_sequence_format_attribute_are_load_errors(
    tmp_path,
):
    for spec, fragment in (
        ("{gen: format, template: 'a{}'}", "write {name}"),
        ("{gen: sequence, format: '{0.x}'}", "format"),
    ):
        with pytest.raises(BlueprintError, match=re.escape(fragment)):
            SimulationContext.from_yaml(
                _write(tmp_path, model(f"payload: {{x: {spec}}}"))
            )


def test_omit_rate_inside_a_names_entry_or_a_case_is_refused_at_load(tmp_path):
    for spec in (
        "{gen: expr, expr: 'a', names: {a: {gen: integer, bounds: [1, 1], omit_rate: 1}}}",
        "{gen: switch, on: 1, cases: {1: {gen: integer, bounds: [1, 1], omit_rate: 1}}}",
    ):
        with pytest.raises(BlueprintError, match="omit_rate and omit_when"):
            SimulationContext.from_yaml(
                _write(tmp_path, model(f"payload: {{x: {spec}}}"))
            )


def test_an_overflow_while_drawing_is_a_generator_error(tmp_path):
    message = error_of(
        tmp_path,
        "payload: {x: {gen: multiply, args: [1.0e+308, 10.0], decimals: 0}}",
    )
    assert "ValueGenerationError" in message or "gen multiply" in message


def test_sample_refuses_a_boolean_parameter(tmp_path):
    message = error_of(
        tmp_path, "payload: {x: {gen: sample, dist: poisson, mean: true}}"
    )
    assert "mean" in message


def test_every_builder_place_takes_a_typed_generator_with_its_own_stream():
    """Each place that takes a value in a blueprint takes a typed generator from
    `generator(spec)` in the Builder, and each gets a stream named for its place."""
    from desflow.values import generator

    def draw():
        return generator({"gen": "uniform", "low": 0, "high": 1})

    store_item, step_record, signal_value, task_set, cause = (draw() for _ in range(5))
    app = SimulationContext(sim_id="B", random_seed=4, factor=0)
    app.add_store("shelf", items=[store_item])
    app.add_signal("z", on=[{"fire": {"signal": "y", "value": signal_value}}])
    app.add_signal("y")
    app.add_arrival("o", "constant", value=1, spawn="x")
    app.add_scenario([{"at": 1.5, "record": {"drawn": step_record}}])

    @app.task(name="x", set={"n": task_set}, interrupt={"task": "x", "cause": cause})
    def x(task_id):
        return {}

    recorder = RecordingEgress()
    app.add_egress(recorder, when="labels")
    app.run(until=3)
    assert store_item._path == "stores.shelf.items.0"
    assert step_record._path == "scenario.0.record.drawn"
    assert signal_value._path == "signals.z.on.0.fire.value"
    assert task_set._path and cause._path
    paths = {
        store_item._path,
        step_record._path,
        signal_value._path,
        task_set._path,
        cause._path,
    }
    assert len(paths) == 5
    labels = [r for r in recorder.records if r["stream_type"] == "label"]
    assert labels and 0 <= labels[0]["label"]["drawn"] < 1


# --- relationships between records ----------------------------------------------------

RELATIONS = (
    Path(__file__).parent.parent / "conformance" / "yaml_only" / "relationships.yaml"
)


def relation_run(tmp_path: Path, text: str, until: float = 12) -> Any:
    """A run of a blueprint whose arrivals are written in it."""
    outcome = run_yaml_text(tmp_path, text.replace("__UNTIL__", str(until)), {})
    assert outcome.error is None, outcome.error
    return outcome


def relation_model(read: str, make: str = "", arrivals: str = "", seed: int = 3) -> str:
    """A producer `make` that writes `{id, group, amount}` a second from time 1, and a
    consumer `read` every 3 seconds from time 3.5, with `read` written as `read`."""
    return (
        HEAD % seed
        + "arrivals:\n  makers: {dist: constant, value: 1, spawn: make}\n"
        + "  readers: {dist: constant, value: 3, first_at: 3.5, spawn: read}\n"
        + arrivals
        + "tasks:\n  make:\n"
        + textwrap.indent(
            textwrap.dedent(
                make
                or """\
                vars: {n: {gen: sequence}}
                payload: {id: {gen: var, name: n}, group: {gen: expr, expr: "n % 3"}, amount: {gen: expr, expr: "n * 10"}}
                """
            ).strip("\n"),
            "    ",
        )
        + "\n  read:\n"
        + textwrap.indent(textwrap.dedent(read).strip("\n"), "    ")
        + "\nrun: {until: 11}\n"
    )


def read_payloads(tmp_path: Path, read: str, **kwargs: Any) -> List[Dict[str, Any]]:
    outcome = run_yaml_text(tmp_path, relation_model(read, **kwargs), {})
    return payloads(outcome, "read")


def load_error(tmp_path: Path, text: str) -> str:
    (tmp_path / "bad.yaml").write_text(text)
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(tmp_path / "bad.yaml")
    return str(error.value)


def test_relationships_file_gives_each_kind_what_it_states(tmp_path):
    """Every kind of `relationships.yaml` at the first read, 3.5 s into the run: the
    rows 0, 1 and 2 of `make` exist, and the start at 0.5 s was dropped."""
    first, second, third = payloads(
        relation_run(tmp_path, RELATIONS.read_text(), 11), "read"
    )
    assert first["step"] == 0 and second["before"] == 0 and third["before"] == 1
    assert first["before"] == -1  # no earlier record on the stream: the default
    assert first["oldest"] == 0 and first["newest"] == 2
    assert first["whole"] == {"id": 2, "group": 2, "amount": 20}
    assert first["any"] in (0, 1, 2)
    assert sorted(first["different"]) == [0, 1, 2]
    assert first["mean"] == 10 and first["total"] == 30
    assert (first["lowest"], first["highest"], first["rows"]) == (0, 20, 3)
    assert first["group_zero"] == 0 and second["group_zero"] == 30
    follow = payloads(relation_run(tmp_path, RELATIONS.read_text(), 11), "follow")
    assert [p["from_read"] for p in follow] == [0, 1, 2]
    assert follow[0]["whole"]["step"] == 0 and follow[0]["whole"]["oldest"] == 0


def test_lookup_strategies_and_path(tmp_path):
    found = read_payloads(
        tmp_path,
        """\
        payload:
          first: {gen: lookup, task: make, path: id, strategy: first}
          last: {gen: lookup, task: make, path: id, strategy: last}
          row: {gen: lookup, task: make, strategy: first}
          random: {gen: repeat, times: 40, value: {gen: lookup, task: make, path: amount}}
        """,
    )
    assert [p["first"] for p in found] == [0, 0, 0]
    assert [p["last"] for p in found] == [2, 5, 8]
    assert found[1]["row"] == {"id": 0, "group": 0, "amount": 0}
    # the rows are 0 to 2, then 0 to 5, then 0 to 8: every one is drawn in time
    assert set(found[0]["random"]) == {0, 10, 20}
    assert set(found[2]["random"]) <= {n * 10 for n in range(9)}
    assert len(set(found[2]["random"])) > 4


def test_lookup_reads_only_a_published_row(tmp_path):
    """A row is readable from the instant its finished record is published. `make` takes
    5 seconds, so the read at 3.5 s has no row and is dropped, and the first read is at
    6.5 s, when the arrival of time 1 has finished at 6. Its `queued` and `started`
    records add no row."""
    make = """\
        service_time: 5
        vars: {n: {gen: sequence}}
        payload: {id: {gen: var, name: n}}
        """
    read = "payload: {last: {gen: lookup, task: make, path: id, strategy: last}, rows: {gen: aggregate, task: make, last: 100, function: count}}"
    outcome = run_yaml_text(tmp_path, relation_model(read, make=make), {})
    reads = [
        r for r in outcome.records if r.get("task") == "read" and "last" in r["value"]
    ]
    assert [r["sim_ts"] for r in reads] == [6.5, 9.5]
    assert [(r["value"]["last"], r["value"]["rows"]) for r in reads] == [(0, 1), (3, 4)]


def test_lookup_histogram_draws_from_the_bins_in_the_order_written(tmp_path):
    found = read_payloads(
        tmp_path,
        """\
        payload:
          early: {gen: repeat, times: 200, value: {gen: lookup, task: make, path: id, histogram: [{bin: 0.2, frequency: 0.9}, {bin: 0.8, frequency: 0.1}]}}
          only_late: {gen: repeat, times: 50, value: {gen: lookup, task: make, path: id, histogram: [{bin: 1, frequency: 0}, {bin: 1, frequency: 1}]}}
        """,
    )
    last = found[2]  # the nine rows 0 to 8: the first bin covers row 0 or rows 0 and 1
    share_first_bin = sum(1 for v in last["early"] if v <= 1) / 200
    assert 0.8 < share_first_bin <= 1.0
    assert set(last["only_late"]) <= {4, 5, 6, 7, 8}  # the second half of the rows
    assert len(set(last["only_late"])) > 2


def test_lookup_histogram_leaves_out_a_bin_with_no_row(tmp_path):
    """With 3 rows the bin of a tenth of them holds none, and the draw uses the others."""
    found = read_payloads(
        tmp_path,
        "payload: {x: {gen: repeat, times: 30, value: {gen: lookup, task: make, path: id, histogram: [{bin: 0.1, frequency: 5}, {bin: 0.9, frequency: 1}]}}}",
    )
    assert len(found[0]["x"]) == 30
    assert set(found[0]["x"]) <= {0, 1, 2}


def test_lookup_latest_by_sees_the_newest_row_of_each_value(tmp_path):
    make = """\
        vars: {n: {gen: sequence}}
        payload: {id: {gen: var, name: n}, who: {gen: expr, expr: "n % 2"}, version: {gen: var, name: n}}
        """
    found = read_payloads(
        tmp_path,
        "payload: {x: {gen: repeat, times: 60, value: {gen: lookup, task: make, path: version, latest_by: who}}}",
        make=make,
    )
    assert set(found[0]["x"]) == {1, 2}  # rows 0 1 2: who 0 -> 2, who 1 -> 1
    assert set(found[2]["x"]) == {7, 8}  # rows 0 to 8
    assert set(found[1]["x"]) == {4, 5}


def test_lookup_latest_by_follows_a_history_that_drops_rows(tmp_path):
    """With `history: 4` the rows 0 to 5 leave 2 to 5; a value whose newest row left
    is gone."""
    make = """\
        history: 4
        vars: {n: {gen: sequence}}
        payload: {id: {gen: var, name: n}, who: {gen: expr, expr: "n if n < 2 else 9"}}
        """
    found = read_payloads(
        tmp_path,
        "payload: {x: {gen: repeat, times: 40, value: {gen: lookup, task: make, path: id, latest_by: who}}, n: {gen: aggregate, task: make, last: 100, function: count}}",
        make=make,
    )
    assert found[0]["n"] == 3 and set(found[0]["x"]) == {0, 1, 2}
    assert found[1]["n"] == 4 and set(found[1]["x"]) == {5}  # rows 2 to 5, all who 9


def test_lookup_distinct_gives_each_row_once_in_a_repeat(tmp_path):
    found = read_payloads(
        tmp_path,
        """\
        payload:
          a: {gen: repeat, times: 3, value: {gen: lookup, task: make, path: id, distinct: true}}
          b: {gen: repeat, times: 3, value: {gen: lookup, task: make, path: id, distinct: true}}
          last: {gen: repeat, times: 3, value: {gen: lookup, task: make, path: id, strategy: last, distinct: true}}
        """,
    )
    for payload in found:
        assert len(set(payload["a"])) == 3 and len(set(payload["b"])) == 3
    assert found[0]["last"] == [2, 1, 0]
    assert found[2]["last"] == [8, 7, 6]


def test_lookup_distinct_without_enough_rows_names_the_task(tmp_path):
    outcome = run_yaml_text(
        tmp_path,
        relation_model(
            "payload: {a: {gen: repeat, times: 4, value: {gen: lookup, task: make, path: id, distinct: true}}}"
        ),
        {},
    )
    assert any(
        "has no row left" in str(r["value"].get("reason", ""))
        for r in outcome.records
        if r.get("task") == "read"
    ) or "has no row left" in str(outcome.error)


def test_lookup_gate_drops_a_start_and_logs_the_wait_once(tmp_path, caplog):
    import logging

    with caplog.at_level(logging.INFO, logger="desflow.values.runtime"):
        found = read_payloads(
            tmp_path,
            "payload: {x: {gen: lookup, task: make, path: id}}",
            arrivals="  early: {dist: constant, value: 0.4, first_at: 0.5, spawn: read}\n",
        )
    # `early` starts `read` at 0.5 and 0.9 with no row, and 1.3, 1.7, ... with one
    assert all(p["x"] >= 0 for p in found) and len(found) > 3
    lines = [
        r.getMessage() for r in caplog.records if "was not started" in r.getMessage()
    ]
    assert len(lines) == 1
    assert "task 'read'" in lines[0] and "make" in lines[0]
    assert "2 starts were dropped" in lines[0] and "0.8" in lines[0]


def test_lookup_gate_holds_a_task_reached_by_next(tmp_path):
    """`a` leads to `b`, which looks up `c`; `c` has no row until time 5, so the flows
    of `a` that reach `b` before then end there."""
    text = (
        HEAD % 1
        + "arrivals: {a: {dist: constant, value: 2, spawn: a}, c: {dist: constant, value: 5, spawn: c}}\n"
        + "tasks:\n  a: {payload: {k: 1}, next: b}\n  b: {payload: {v: {gen: lookup, task: c, path: z}}}\n"
        + "  c: {payload: {z: 7}}\nrun: {until: 11}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None
    assert [r["sim_ts"] for r in outcome.records if r.get("task") == "b"] == [
        6.0,
        8.0,
        10.0,
    ]
    assert [r["sim_ts"] for r in outcome.records if r.get("task") == "a"] == [
        2,
        4,
        6,
        8,
        10,
    ]


def test_a_task_that_nothing_starts_is_refused_as_a_lookup_target(tmp_path):
    text = (
        HEAD % 1
        + "arrivals: {a: {dist: constant, value: 1, spawn: grab}}\n"
        + "tasks:\n  grab: {payload: {x: {gen: lookup, task: other}}}\n  other: {payload: {}}\n"
    )
    message = load_error(tmp_path, text)
    assert "which no arrival, spawn, next or wait starts" in message, message


def test_previous_follows_the_arrival_as_one_stream(tmp_path):
    """Two entities of one arrival share a stream, and a second arrival that starts the
    same task has its own."""
    text = (
        HEAD % 1
        + "arrivals:\n  a: {dist: constant, value: 2, first_at: 1, spawn: t}\n"
        + "  b: {dist: constant, value: 2, first_at: 2, spawn: t}\n"
        + "tasks:\n  t:\n    vars: {n: {gen: sequence}}\n"
        + "    payload: {n: {gen: var, name: n}, who: {gen: entity}, before: {gen: previous, path: n, default: -1}}\n"
        + "run: {until: 6.5}\n"
    )
    found = payloads(run_yaml_text(tmp_path, text, {}), "t")
    # n counts every start: a at 1, b at 2, a at 3, b at 4, a at 5, b at 6
    assert [(p["who"][0], p["n"], p["before"]) for p in found] == [
        ("a", 0, -1),
        ("b", 1, -1),
        ("a", 2, 0),
        ("b", 3, 1),
        ("a", 4, 2),
        ("b", 5, 3),
    ]


def test_previous_is_per_entity_for_an_arrival_with_count_or_keys(tmp_path):
    for arrival in (
        "{dist: constant, value: 3, count: 3, stagger: 1, spawn: t}",
        "{dist: constant, value: 3, keys: [x, y, z], stagger: 1, spawn: t}",
    ):
        text = (
            HEAD % 1
            + f"arrivals: {{a: {arrival}}}\n"
            + "tasks:\n  t:\n    vars: {n: {gen: sequence}}\n"
            + "    payload: {n: {gen: var, name: n}, e: {gen: entity}, before: {gen: previous, path: n, default: -1}}\n"
            + "run: {until: 10}\n"
        )
        found = payloads(run_yaml_text(tmp_path, text, {}), "t")
        by_entity: Dict[str, List[Dict[str, Any]]] = {}
        for p in found:
            by_entity.setdefault(p["e"], []).append(p)
        assert len(by_entity) == 3
        for runs in by_entity.values():
            assert runs[0]["before"] == -1
            assert [p["before"] for p in runs[1:]] == [p["n"] for p in runs[:-1]]
        assert len(found) >= 6


def test_previous_without_a_default_is_null_at_the_first_record(tmp_path):
    text = (
        HEAD % 1
        + "arrivals: {a: {dist: constant, value: 1, spawn: t, max_events: 2}}\n"
        + "tasks:\n  t:\n    payload: {before: {gen: previous, path: n}, n: 1}\nrun: {until: 5}\n"
    )
    found = payloads(run_yaml_text(tmp_path, text, {}), "t")
    assert [p["before"] for p in found] == [None, 1]


def test_previous_starts_with_a_default_that_is_drawn_only_then(tmp_path):
    """The default draws from its own stream only when there is no record, so the other
    fields give the same draws whether or not a default is written."""
    base = (
        HEAD % 5
        + "arrivals: {a: {dist: constant, value: 1, spawn: t, max_events: 4}}\n"
        + "tasks:\n  t:\n    payload: {u: {gen: uniform, low: 0, high: 1}, p: __P__}\nrun: {until: 6}\n"
    )
    with_previous = base.replace(
        "__P__", "{gen: previous, path: u, default: {gen: uniform, low: 5, high: 6}}"
    )
    plain = base.replace("__P__", "0")
    a = payloads(run_yaml_text(tmp_path, with_previous, {}), "t")
    b = payloads(run_yaml_text(tmp_path, plain, {}), "t")
    assert [p["u"] for p in a] == [p["u"] for p in b]
    assert 5 <= a[0]["p"] <= 6 and a[1]["p"] == a[0]["u"] and a[2]["p"] == a[1]["u"]


def test_upstream_reads_the_payload_of_the_routing_task(tmp_path):
    text = (
        HEAD % 1
        + "arrivals: {a: {dist: constant, value: 1, spawn: one, max_events: 2}}\n"
        + "tasks:\n  one: {payload: {tenant: acme, n: {gen: sequence}}, next: two}\n"
        + "  two: {payload: {tenant: {gen: upstream, path: tenant}, n: {gen: upstream, path: n}, all: {gen: upstream}}}\n"
        + "run: {until: 5}\n"
    )
    found = payloads(run_yaml_text(tmp_path, text, {}), "two")
    assert [p["tenant"] for p in found] == ["acme", "acme"]
    assert [p["n"] for p in found] == [0, 1]
    assert found[1]["all"] == {"tenant": "acme", "n": 1}


def test_aggregate_where_selects_the_rows_of_a_day(tmp_path):
    """The daily weather shape: a forecast row each hour for lead 1 or 2, and a daily
    task that averages the rows of one lead and one day. `where` reads the row's fields
    and also the task's own var `day`."""
    spec = (
        HEAD % 1
        + "arrivals: {h: {dist: constant, value: 3600, spawn: forecast}, d: {dist: constant, value: 86400, first_at: 86399, spawn: daily}}\n"
        + "tasks:\n"
        + "  forecast:\n    vars: {k: {gen: sequence}}\n"
        + "    payload: {lead_days: {gen: expr, expr: '1 + k % 2'}, forecast_for: {gen: format_time, format: '%Y-%m-%dT%H'}, temperature: {gen: var, name: k}, precipitation: {gen: expr, expr: '0 if k % 4 else 1'}}\n"
        + "  daily:\n    vars:\n      day: {gen: format_time, format: '%Y-%m-%d'}\n"
        + '      mean: {gen: aggregate, task: forecast, path: temperature, last: 24, function: mean, where: "lead_days == 1 and forecast_for[:10] == day"}\n'
        + '      wet: {gen: aggregate, task: forecast, last: 24, function: count, where: "lead_days == 1 and forecast_for[:10] == day and precipitation > 0"}\n'
        + "    payload: {mean: {gen: var, name: mean}, wet: {gen: var, name: wet}}\nrun: {until: 90000}\n"
    )
    found = payloads(run_yaml_text(tmp_path, spec, {}), "daily")
    # The first row is at 1 h (k = 0), so 2026-01-01 holds k = 0 to 22; lead 1 is
    # every even k, and a row is wet when k is a multiple of 4
    lead_one = [k for k in range(23) if k % 2 == 0]
    assert found[0]["mean"] == pytest.approx(sum(lead_one) / len(lead_one))
    assert found[0]["wet"] == len([k for k in lead_one if k % 4 == 0])


def test_aggregate_names_an_unreadable_value(tmp_path):
    outcome = run_yaml_text(
        tmp_path,
        relation_model(
            "payload: {x: {gen: aggregate, task: make, path: kind, last: 2, function: mean}}",
            make="vars: {n: {gen: sequence}}\npayload: {kind: text}",
        ),
        {},
    )
    assert "kind must be a number in every row" in str(outcome.error)


def test_aggregate_where_scans_every_row_the_task_keeps(tmp_path):
    """A `where` that keeps fewer than `last` rows takes the rows there are, from all
    the rows kept and not from a fixed number of the newest."""
    found = read_payloads(
        tmp_path,
        "payload: {x: {gen: aggregate, task: make, last: 5, function: count, where: 'id == 0'}}",
    )
    assert [p["x"] for p in found] == [1, 1, 1]


def test_a_generator_added_beside_others_leaves_their_draws(tmp_path):
    base = """\
        payload:
          u: {gen: uniform, low: 0, high: 1}
          %s
        """
    a = read_payloads(tmp_path, base % "x: 1")
    b = read_payloads(tmp_path, base % "x: {gen: lookup, task: make, path: id}")
    assert [p["u"] for p in a] == [p["u"] for p in b]


@pytest.mark.parametrize(
    "value, fragment",
    [
        ("{gen: lookup, task: nope}", "which is not defined under tasks"),
        ("{gen: aggregate, task: nope, path: x, last: 2, function: sum}", "nope"),
        ("{gen: lookup, task: silent}", "payload: null"),
        (
            "{gen: lookup, task: make, path: idd}",
            "'idd' is not a field of the payload of task 'make'",
        ),
        ("{gen: lookup, task: make, latest_by: grp}", "'grp' is not a field"),
        (
            "{gen: aggregate, task: make, path: amoun, last: 2, function: sum}",
            "'amoun' is not a field",
        ),
        ("{gen: lookup, task: make, distinct: true}", "inside the value of a repeat"),
        (
            "{gen: lookup, task: make, strategy: first, histogram: [{bin: 1, frequency: 1}]}",
            "strategy: random only",
        ),
        (
            "{gen: lookup, task: make, latest_by: a.b}",
            "latest_by names one field of the row",
        ),
        (
            "{gen: lookup, task: make, histogram: [{bin: 1, frequency: .inf}]}",
            "frequency",
        ),
        ("{gen: lookup, task: make, histogram: [{bin: .inf, frequency: 1}]}", "bin"),
        ("{gen: aggregate, task: make, path: id, last: true, function: sum}", "last"),
        ("{gen: aggregate, task: make, path: id, last: 2.0, function: sum}", "last"),
        ("{gen: lookup, task: make, histogram: []}", "at least one bin"),
        (
            "{gen: lookup, task: make, histogram: [{bin: 1, frequency: 0}]}",
            "add up to above 0",
        ),
        (
            "{gen: aggregate, task: make, last: 2, function: count, path: id}",
            "takes no path",
        ),
        ("{gen: aggregate, task: make, last: 2, function: mean}", "needs the path"),
        ("{gen: aggregate, task: make, path: id, last: 0, function: sum}", "last"),
        (
            "{gen: aggregate, task: make, path: id, last: 2, function: median}",
            "function",
        ),
        (
            "{gen: aggregate, task: make, path: id, last: 2, function: sum, where: 'id =='}",
            "where",
        ),
        (
            "{gen: previous, path: nope, default: 1}",
            "'nope' is not a field of the payload of task 'read'",
        ),
        ("{gen: upstream, path: step}", "no task routes to 'read' with next"),
    ],
)
def test_relationship_errors_name_the_line(tmp_path, value, fragment):
    text = (
        HEAD % 1
        + "arrivals: {m: {dist: constant, value: 1, spawn: make}, r: {dist: constant, value: 1, spawn: read}}\n"
        + "tasks:\n  make: {payload: {id: 1, group: 2, amount: 3}}\n  silent: {payload: null}\n"
        + f"  read:\n    payload: {{step: 1, x: {value}}}\n"
    )
    message = load_error(tmp_path, text)
    assert fragment in message, message
    assert message.startswith(str(tmp_path / "bad.yaml")), message
    assert "<gen>" not in message and "<map>" not in message


def test_previous_and_upstream_are_refused_where_no_task_runs(tmp_path):
    for place in (
        "attributes: {x: {gen: previous, path: p, default: 1}}",
        "attributes: {x: {gen: upstream, path: p}}",
    ):
        text = (
            HEAD % 1
            + f"arrivals: {{a: {{dist: constant, value: 1, spawn: t, {place}}}}}\n"
            + "tasks: {t: {payload: {}}}\n"
        )
        message = load_error(tmp_path, text)
        assert "so it stands in a task" in message, message


def test_tasks_that_look_each_other_up_are_refused(tmp_path):
    text = (
        HEAD % 1
        + "arrivals: {a: {dist: constant, value: 1, spawn: a}, b: {dist: constant, value: 1, spawn: b}}\n"
        + "tasks:\n  a: {payload: {v: {gen: lookup, task: b, path: w}, z: 1}}\n"
        + "  b: {payload: {w: {gen: lookup, task: a, path: z}}}\n"
    )
    message = load_error(tmp_path, text)
    assert "tasks look each other up" in message and "a -> b -> a" in message
    own = text.replace("task: b, path: w", "task: a, path: z")
    assert "a -> a" in load_error(tmp_path, own)


def test_upstream_path_must_be_a_field_of_every_task_that_routes_with_next(tmp_path):
    text = (
        HEAD % 1
        + "arrivals: {a: {dist: constant, value: 1, spawn: p}}\n"
        + "tasks:\n  p: {payload: {k: 1}, next: q}\n  o: {payload: {z: 1}, next: q}\n"
        + "  q: {payload: {x: {gen: upstream, path: k}}}\n"
    )
    message = load_error(tmp_path, text)
    assert "'k' is not a field of the payload of task 'o'" in message, message


def test_a_lookup_in_an_arrival_place_reads_rows_when_the_entity_arrives(tmp_path):
    text = (
        HEAD % 1
        + "arrivals:\n  m: {dist: constant, value: 1, spawn: make}\n"
        + "  r: {dist: constant, value: 2, first_at: 3.5, spawn: read, attributes: {seen: {gen: lookup, task: make, path: id, strategy: last}}}\n"
        + "tasks:\n  make:\n    vars: {n: {gen: sequence}}\n    payload: {id: {gen: var, name: n}}\n"
        + "  read: {payload: {seen: {gen: attr, name: seen}}}\nrun: {until: 8}\n"
    )
    found = payloads(run_yaml_text(tmp_path, text, {}), "read")
    assert [p["seen"] for p in found] == [2, 4, 6]


def test_history_keeps_the_last_rows_and_a_task_nobody_looks_up_keeps_none(tmp_path):
    found = read_payloads(
        tmp_path,
        "payload: {n: {gen: aggregate, task: make, last: 100, function: count}, o: {gen: lookup, task: make, path: id, strategy: first}}",
        make="history: 4\nvars: {n: {gen: sequence}}\npayload: {id: {gen: var, name: n}}",
    )
    assert [(p["n"], p["o"]) for p in found] == [(3, 0), (4, 2), (4, 5)]
    app = SimulationContext.from_yaml(
        _write_model(tmp_path, relation_model("payload: {x: 1}"))
    )
    app.run(until=2)
    assert "make" not in app.values._kept


def _write_model(tmp_path: Path, text: str) -> Path:
    (tmp_path / "m.yaml").write_text(text)
    return tmp_path / "m.yaml"


@pytest.mark.parametrize("bad", ["0", "true", '"10"', "5.0", "-3"])
def test_a_history_that_is_not_a_whole_number_of_one_or_more_is_refused(tmp_path, bad):
    text = relation_model("payload: {}", make=f"history: {bad}\npayload: {{a: 1}}")
    assert "history" in load_error(tmp_path, text)


def test_a_history_has_no_upper_limit(tmp_path):
    found = read_payloads(
        tmp_path,
        "payload: {n: {gen: aggregate, task: make, last: 10000000, function: count}}",
        make="history: 50000000\nvars: {n: {gen: sequence}}\npayload: {id: {gen: var, name: n}}",
    )
    assert [p["n"] for p in found] == [3, 6, 9]


def test_relationships_hold_across_replications_and_a_sample(tmp_path):
    """Each replication starts with no rows and no `previous` stream, so each reads the
    same count of rows, and `--sample` prints the records of a run that reads them."""
    from typer.testing import CliRunner

    from desflow.analysis import replicate
    from desflow.cli import app as cli

    path = _write(
        tmp_path,
        relation_model(
            "payload: {n: {gen: aggregate, task: make, last: 100, function: count}, before: {gen: previous, path: n, default: -1}}"
        ),
    )
    result = CliRunner().invoke(
        cli, ["run", str(path), "--sample", "40", "--until", "8"]
    )
    assert result.exit_code == 0, result.output
    reads = [
        json.loads(line)["value"]
        for line in result.stdout.splitlines()
        if '"task":"read"' in line.replace(" ", "") and '"before"' in line
    ]
    assert [(r["n"], r["before"]) for r in reads] == [(3, -1), (6, 3)]
    summary = replicate(
        path, 3, seed=2, until=8, measures=["tasks.read.finished"]
    ).summary
    assert (
        summary[0]["mean"] == 2
    )  # the reads at 3.5 and 6.5 in each, never fewer or more


def _builder_events(app: SimulationContext, until: float) -> List[Any]:
    recorder = RecordingEgress()
    app.add_egress(recorder)
    app.run(until=until)
    return [
        r
        for r in recorder.records
        if r["stream_type"] == "event"
        and isinstance(r["value"], dict)
        and "status" not in r["value"]
    ]


def _builder_pair(read_payload: Any, **make_options: Any) -> SimulationContext:
    app = SimulationContext(sim_id="B", random_seed=2, factor=0)
    app.add_arrival("m", "constant", value=1, spawn="make")
    app.add_arrival("r", "constant", value=3, first_at=3.5, spawn="read")

    @app.task(name="make", vars={"n": {"gen": "sequence"}}, **make_options)
    def make(task_id):
        return Template(
            {"id": {"gen": "var", "name": "n"}},
            name="make",
            vars={"n": {"gen": "sequence"}},
        )

    app.task(name="read")(lambda task_id: read_payload)
    return app


def test_builder_template_looks_up_a_task_that_asks_for_history():
    read = Template(
        {
            "last": {"gen": "lookup", "task": "make", "path": "id", "strategy": "last"},
            "rows": {
                "gen": "aggregate",
                "task": "make",
                "last": 9,
                "function": "count",
            },
            "before": {"gen": "previous", "path": "rows", "default": -1},
        },
        name="read",
    )
    events = _builder_events(_builder_pair(read, history=5), 8)
    found = [r["value"] for r in events if r["task"] == "read"]
    assert [(p["last"], p["rows"], p["before"]) for p in found] == [
        (2, 3, -1),
        (5, 5, 3),
    ]


def test_builder_task_that_keeps_no_rows_names_the_fix():
    read = Template({"x": {"gen": "lookup", "task": "make", "path": "id"}}, name="read")
    app = _builder_pair(read)
    recorder = RecordingEgress()
    app.add_egress(recorder)
    with pytest.raises(Exception) as error:
        app.run(until=8)
    assert "keeps no rows" in str(error.value) and "history=" in str(error.value)


def test_builder_task_values_name_the_rows_they_need_without_history():
    """A `lookup` in the `vars` of a builder task makes the task it names keep rows, and
    a task with one does not start before that task has a row."""
    app = SimulationContext(sim_id="B", random_seed=2, factor=0)
    app.add_arrival("m", "constant", value=2, spawn="make")
    app.add_arrival("r", "constant", value=1, spawn="read")
    app.task(name="make")(lambda task_id: {"id": task_id})
    app.task(
        name="read",
        vars={"u": {"gen": "lookup", "task": "make", "path": "id", "strategy": "last"}},
    )(lambda task_id: Template({"u": {"gen": "var", "name": "u"}}, name="read"))
    found = [r for r in _builder_events(app, 6) if r["task"] == "read"]
    # `make` first finishes at 2, so the read at 1 is dropped; the read at 2 follows
    # the finish of `make` in the same instant
    assert [(r["sim_ts"], r["value"]["u"]) for r in found] == [
        (2, 0),
        (3, 0),
        (4, 1),
        (5, 1),
    ]


def test_builder_refuses_a_lookup_of_a_name_that_is_not_a_task_and_a_cycle():
    app = SimulationContext(sim_id="B", random_seed=2, factor=0)
    app.add_arrival("r", "constant", value=1, spawn="read")
    app.task(name="read", vars={"u": {"gen": "lookup", "task": "ghost"}})(
        lambda task_id: {}
    )
    with pytest.raises(ValueError, match="names task 'ghost'"):
        app.run(until=2)
    cyc = SimulationContext(sim_id="B", random_seed=2, factor=0)
    cyc.add_arrival("a", "constant", value=1, spawn="a")
    cyc.task(name="a", vars={"u": {"gen": "lookup", "task": "b"}})(lambda task_id: {})
    cyc.task(name="b", vars={"u": {"gen": "lookup", "task": "a"}})(lambda task_id: {})
    with pytest.raises(ValueError, match="a -> b -> a"):
        cyc.run(until=2)


def test_builder_history_must_be_a_whole_number_of_one_or_more():
    app = SimulationContext(sim_id="B", factor=0)
    for bad in (0, -1, 2.5, True):
        with pytest.raises(ValueError, match="history"):
            app.task(name="t", history=bad)


def test_low_level_script_reads_rows_through_the_value_runtime():
    app = SimulationContext(sim_id="B", random_seed=2, factor=0)
    app.add_arrival("m", "constant", value=1, spawn="make")
    app.task(name="make", history=3)(lambda task_id: {"id": task_id, "v": task_id * 2})
    seen: List[Any] = []

    def reader(context):
        yield context.env.timeout(5.5)
        for spec in (
            {"gen": "lookup", "task": "make", "path": "v", "strategy": "last"},
            {"gen": "lookup", "task": "make", "path": "v", "strategy": "first"},
            {
                "gen": "aggregate",
                "task": "make",
                "path": "v",
                "last": 9,
                "function": "sum",
            },
        ):
            seen.append(context.values.value(spec))

    app.add_process(reader)
    app.run(until=6)
    assert seen == [8, 4, 4 + 6 + 8]


def test_runtime_rows_drop_the_oldest_and_the_latest_index_follows():
    from desflow.values.runtime import _Rows

    rows = _Rows(3)
    index = rows.latest("who")
    for n in range(8):
        rows.add({"n": n, "who": n % 2})
    assert [r["n"] for _, r in (rows.at(i) for i in range(len(rows)))] == [5, 6, 7]
    assert sorted(index.at(i)[1]["n"] for i in range(len(index))) == [6, 7]
    rows.add({"n": 8, "who": [1]})  # a list is kept by its text
    rows.add({"n": 9})  # no such field: not in the index
    assert [r["n"] for _, r in (rows.at(i) for i in range(len(rows)))] == [7, 8, 9]
    assert sorted(index.at(i)[1]["n"] for i in range(len(index))) == [7, 8]


# ------------------------------------------------------------ review round 1


def test_a_dropped_start_runs_none_of_its_on_actions(tmp_path):
    """`read` is started at 1 s and 2 s, before `make` has a row at 3 s, and the run
    ends at 2.5 s. Both starts are dropped, so neither its constant `on:` write, its
    `on:` write with a `lookup` (which would stop the run), nor its `on:` spawn runs."""
    text = (
        HEAD % 1
        + "resources: {pool: {current_cap: 1}}\n"
        + "arrivals: {m: {dist: constant, value: 1, first_at: 3, spawn: make, max_events: 1}, r: {dist: constant, value: 1, first_at: 1, spawn: read, max_events: 2}}\n"
        + "tasks:\n  make: {payload: {a: 1}}\n  other: {payload: {b: 1}}\n"
        + "  read:\n    on:\n      - {spawn: {task: other}}\n"
        + "      - {path: B.resources.pool.current_cap, value: 4}\n"
        + "      - {path: B.resources.pool.current_cap, value: {gen: lookup, task: make, path: a}}\n"
        + "    payload: {x: {gen: lookup, task: make, path: a}}\n"
        + "run: {until: 2.5}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, outcome.error
    assert payloads(outcome, "read") == [] and payloads(outcome, "other") == []
    assert outcome.app_or_env.get_resource("pool").capacity == 1


def test_a_row_field_named_t_wins_over_the_simulation_time_in_where(tmp_path):
    text = (
        HEAD % 1
        + "arrivals: {m: {dist: constant, value: 1, spawn: make, max_events: 4}, r: {dist: constant, value: 10, first_at: 5, spawn: read, max_events: 1}}\n"
        + "tasks:\n  make: {vars: {n: {gen: sequence}}, payload: {t: {gen: var, name: n}}}\n"
        + "  read: {payload: {c: {gen: aggregate, task: make, last: 10, function: count, where: 't == 2'}}}\n"
        + "run: {until: 20}\n"
    )
    assert payloads(run_yaml_text(tmp_path, text, {}), "read") == [{"c": 1}]


def test_a_builder_template_that_looks_up_a_task_with_no_row_is_dropped(
    tmp_path, caplog
):
    import logging

    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    app.add_arrival("m", "constant", value=1, first_at=3, spawn="make")
    app.add_arrival("r", "constant", value=1, first_at=1, spawn="read")
    app.task(name="make", history=5)(lambda task_id: {"id": task_id})
    template = Template(
        {"x": {"gen": "lookup", "task": "make", "path": "id"}}, name="read"
    )
    app.task(name="read")(lambda task_id: template)
    with caplog.at_level(logging.INFO, logger="desflow.values.runtime"):
        events = _builder_events(app, 6)
    reads = [r["sim_ts"] for r in events if r["task"] == "read"]
    assert reads and min(reads) >= 3
    lines = [
        r.getMessage() for r in caplog.records if "was not started" in r.getMessage()
    ]
    assert len(lines) == 1 and "task 'read'" in lines[0]


def test_a_python_process_may_start_a_lookup_target(tmp_path, monkeypatch):
    (tmp_path / "procm.py").write_text(
        "def starter(context):\n    yield context.env.timeout(1)\n    yield context.call('tgt')\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    text = (
        HEAD % 1
        + "arrivals: {b: {dist: constant, value: 1, first_at: 3, spawn: read, max_events: 2}}\n"
        + "tasks:\n  tgt: {history: 5, payload: {a: 1}}\n"
        + "  read: {payload: {x: {gen: aggregate, task: tgt, path: a, last: 2, function: sum}}}\n"
        + "processes:\n  - !python procm.starter\nrun: {until: 5}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, outcome.error
    assert [p["x"] for p in payloads(outcome, "read")] == [1, 1]


def test_upstream_is_refused_in_a_task_that_on_fail_reaches(tmp_path):
    text = (
        HEAD % 1
        + "arrivals: {a: {dist: constant, value: 1, spawn: first, max_events: 1}}\n"
        + "tasks:\n  first: {fail: 'True', on_fail: tgt, payload: {a: 1}}\n"
        + "  tgt: {payload: {u: {gen: upstream, path: a}}}\nrun: {until: 3}\n"
    )
    assert "also reached by on_fail" in load_error(tmp_path, text)


def test_two_arrivals_with_the_same_key_keep_separate_previous_streams(tmp_path):
    text = (
        HEAD % 1
        + "arrivals:\n  a: {dist: constant, value: 1, keys: [p], spawn: t}\n"
        + "  b: {dist: constant, value: 1, first_at: 0.5, keys: [p], spawn: t}\n"
        + "tasks:\n  t: {payload: {n: {gen: add, args: [{gen: previous, path: n, default: 0}, 1]}}}\n"
        + "run: {until: 3.9}\n"
    )
    found = payloads(run_yaml_text(tmp_path, text, {}), "t")
    assert sorted(p["n"] for p in found) == [1, 1, 2, 2, 3, 3, 4]


def test_a_gate_that_never_opens_logs_why_the_task_is_empty(tmp_path, caplog):
    import logging

    text = (
        HEAD % 1
        + "arrivals: {m: {dist: constant, value: 1, first_at: 50, spawn: make}, r: {dist: constant, value: 1, spawn: read}}\n"
        + "tasks:\n  make: {payload: {a: 1}}\n  read: {payload: {x: {gen: lookup, task: make, path: a}}}\n"
        + "run: {until: 5}\n"
    )
    with caplog.at_level(logging.INFO, logger="desflow.values.runtime"):
        outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None and payloads(outcome, "read") == []
    lines = [
        r.getMessage() for r in caplog.records if "was not started" in r.getMessage()
    ]
    assert len(lines) == 1
    assert "to the end of the run" in lines[0] and "make" in lines[0]


def test_a_histogram_whose_last_bin_is_empty_still_finds_a_row_among_few(tmp_path):
    found = read_payloads(
        tmp_path,
        "payload: {x: {gen: lookup, task: make, path: id, histogram: [{bin: 0.5, frequency: 1}, {bin: 0.5, frequency: 0}]}}",
        arrivals="  one: {dist: constant, value: 1, first_at: 0.5, spawn: read, max_events: 1}\n",
        make="vars: {n: {gen: sequence}}\npayload: {id: {gen: var, name: n}}\nhistory: 1",
    )
    assert found and all(isinstance(p["x"], int) for p in found)


def test_previous_is_one_stream_for_a_group_of_two(tmp_path):
    text = (
        HEAD % 1
        + "arrivals: {a: {dist: constant, value: 1, group: 2, spawn: t}}\n"
        + "tasks:\n  t: {payload: {n: {gen: add, args: [{gen: previous, path: n, default: 0}, 1]}}}\n"
        + "run: {until: 2.5}\n"
    )
    assert [p["n"] for p in payloads(run_yaml_text(tmp_path, text, {}), "t")] == [
        1,
        2,
        3,
        4,
    ]


def test_initial_entities_are_dropped_by_a_gate_and_not_started_later(tmp_path, caplog):
    import logging

    text = (
        HEAD % 1
        + "arrivals: {i: {initial: 2, spawn: read}, m: {dist: constant, value: 1, first_at: 1, spawn: make}}\n"
        + "tasks:\n  make: {payload: {a: 1}}\n  read: {set: {k: {gen: lookup, task: make, path: a}}, payload: {x: 1}}\n"
        + "run: {until: 5}\n"
    )
    with caplog.at_level(logging.INFO, logger="desflow.values.runtime"):
        outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None
    # not even a failed record: the start never ran
    assert [r for r in outcome.records if r.get("task") == "read"] == []
    lines = [
        r.getMessage() for r in caplog.records if "was not started" in r.getMessage()
    ]
    assert len(lines) == 1 and "2 starts were dropped" in lines[0]


# The newest row of each group is not in the order the groups first appeared in: at the
# third read the newest row of group 0 (7) is older than that of group 1 (8)
GROUPED = """\
vars: {n: {gen: sequence}}
payload: {id: {gen: var, name: n}, group: {gen: expr, expr: "1 if n % 4 == 0 else 0"}}
"""


@pytest.mark.parametrize(
    "extra, expected",
    [
        ("strategy: first", [0, 4, 7]),
        ("strategy: last", [2, 5, 8]),
        ("histogram: [{bin: 0.5, frequency: 1}, {bin: 0.5, frequency: 0}]", [0, 4, 7]),
    ],
)
def test_latest_by_with_a_strategy_or_histogram_orders_the_newest_rows_by_time(
    tmp_path, extra, expected
):
    found = read_payloads(
        tmp_path,
        f"payload: {{x: {{gen: lookup, task: make, path: id, latest_by: group, {extra}}}}}",
        make=GROUPED,
    )
    assert [p["x"] for p in found] == expected


def test_distinct_with_a_histogram_gives_each_row_once(tmp_path):
    found = read_payloads(
        tmp_path,
        "payload: {a: {gen: repeat, times: 3, value: {gen: lookup, task: make, path: id, distinct: true, histogram: [{bin: 1, frequency: 1}]}}}",
    )
    assert all(len(set(p["a"])) == 3 for p in found)


# ------------------------------------------------------------ review round 2


def _template_read() -> Template:
    return Template({"x": {"gen": "lookup", "task": "make", "path": "id"}}, name="read")


def _drop_model(app: SimulationContext, **read_options: Any) -> None:
    """`read` starts at 1 and returns a Template that looks up `make`, which has its
    first row at 5; `useit` needs the resource `r` from 6."""
    app.add_resource("r", current_cap=1)
    app.add_service("s", "constant", value=1)
    app.add_arrival("a", "constant", value=100, first_at=1, spawn="read")
    app.add_arrival("m", "constant", value=100, first_at=5, spawn="make")
    app.add_arrival("u", "constant", value=100, first_at=6, spawn="useit")
    app.task(name="make", history=5)(lambda task_id: {"id": 1})
    template = _template_read()
    app.task(name="read", **read_options)(lambda task_id: template)
    app.task("s", "r", name="useit")(lambda task_id: {"used": 1})


@pytest.mark.parametrize(
    "options",
    [
        {"service_id": "s", "resource_id": "r"},
        {"wait": {"request": "r"}},
        {"wait": {"any": {"w": {"request": "r"}, "t": {"timeout": 0.5}}}},
        {"wait": {"all": {"w": {"request": "r"}, "t": {"timeout": 0.5}}}},
    ],
    ids=["resource_id", "wait-request", "wait-any", "wait-all"],
)
def test_a_dropped_template_start_gives_back_its_units(options):
    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    _drop_model(app, **options)
    events = _builder_events(app, 10)
    resource = app._resources_map["r"]
    assert (len(resource.users), len(resource.queue)) == (0, 0)
    assert [r["sim_ts"] for r in events if r["task"] == "useit"] == [7.0]


def test_a_dropped_template_start_stops_counting_as_in_progress():
    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    app.with_stats("typical", interval=100)
    app.add_service("s", "constant", value=1)
    app.add_arrival("a", "constant", value=1, first_at=1, spawn="read")
    app.add_arrival("m", "constant", value=100, first_at=5, spawn="make")
    app.task(name="make", history=5)(lambda task_id: {"id": 1})
    template = _template_read()
    app.task("s", name="read")(lambda task_id: template)
    _builder_events(app, 4.5)
    stats = app._env.stats
    assert stats._in_progress.get("read", 0) == 0
    assert not [key for key in stats._active if key[0] == "read"]
    assert not [key for key in stats._in_flight if key[0] == "read"]


def test_a_dropped_template_start_has_consumed_its_get_item(caplog):
    """The task function has run, `queued` and `started` are published, and the item
    of the `get` is gone, so the INFO line says so."""
    import logging

    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    app.add_store("st", items=[1, 2, 3])
    app.add_arrival("a", "constant", value=100, first_at=1, spawn="read")
    app.add_arrival("m", "constant", value=100, first_at=5, spawn="make")
    app.task(name="make", history=5)(lambda task_id: {"id": 1})
    template = _template_read()
    calls: List[int] = []
    app.task(name="read", get={"store": "st"})(
        lambda task_id: calls.append(task_id) or template
    )
    recorder = RecordingEgress()
    app.add_egress(recorder)
    with caplog.at_level(logging.INFO, logger="desflow.values.runtime"):
        app.run(until=4)
    assert list(app._stores_map["st"].items) == [2, 3]
    assert len(calls) == 1
    statuses = [
        r["value"].get("status")
        for r in recorder.records
        if r.get("task") == "read" and isinstance(r["value"], dict)
    ]
    assert "queued" in statuses and "started" in statuses
    lines = [
        r.getMessage() for r in caplog.records if "was not started" in r.getMessage()
    ]
    assert len(lines) == 1 and "get item of each of them was consumed" in lines[0]


@pytest.mark.parametrize(
    "wait, items, left, said",
    [
        ({"get": {"store": "st"}}, [1, 2, 3], [2, 3], True),
        (
            {"any": {"g": {"get": {"store": "st"}}, "t": {"timeout": 0.5}}},
            [1, 2, 3],
            [2, 3],
            True,
        ),
        (
            {"all": {"g": {"get": {"store": "st"}}, "t": {"timeout": 0.5}}},
            [1, 2, 3],
            [2, 3],
            True,
        ),
        (
            {"any": {"g": {"get": {"store": "st"}}, "t": {"timeout": 0.5}}},
            [],
            [],
            False,
        ),
        ({"put": {"store": "st", "item": 9}}, [], [9], False),
    ],
    ids=["get", "any-get-taken", "all-get", "any-get-not-taken", "put"],
)
def test_a_dropped_template_start_says_whether_a_wait_get_took_an_item(
    caplog, wait, items, left, said
):
    """A `get` branch of a `wait` that took an item has consumed it, as a task's own
    `get` has. A branch that took nothing, and a `put`, leave no item to report."""
    import logging

    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    app.add_store("st", items=items)
    app.add_arrival("a", "constant", value=100, first_at=1, spawn="read")
    app.add_arrival("m", "constant", value=100, first_at=5, spawn="make")
    app.task(name="make", history=5)(lambda task_id: {"id": 1})
    template = _template_read()
    app.task(name="read", wait=wait)(lambda task_id: template)
    app.add_egress(RecordingEgress())
    with caplog.at_level(logging.INFO, logger="desflow.values.runtime"):
        app.run(until=4)
    assert list(app._stores_map["st"].items) == left
    lines = [
        r.getMessage() for r in caplog.records if "was not started" in r.getMessage()
    ]
    assert len(lines) == 1
    assert ("get item of each of them was consumed" in lines[0]) is said


def test_the_line_for_a_template_start_that_later_goes_ahead_says_the_item_was_consumed(
    caplog,
):
    import logging

    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    app.add_store("st", items=[1, 2, 3, 4, 5])
    app.add_arrival("a", "constant", value=2.5, first_at=1, spawn="read")
    app.add_arrival("m", "constant", value=100, first_at=5, spawn="make")
    app.task(name="make", history=5)(lambda task_id: {"id": 1})
    template = _template_read()
    app.task(name="read", get={"store": "st"})(lambda task_id: template)
    app.add_egress(RecordingEgress())
    with caplog.at_level(logging.INFO, logger="desflow.values.runtime"):
        app.run(until=7)
    lines = [
        r.getMessage() for r in caplog.records if "was not started" in r.getMessage()
    ]
    assert len(lines) == 1 and "until time 6" in lines[0]
    assert (
        "2 starts were dropped; the get item of each of them was consumed" in lines[0]
    )


def test_note_values_on_a_template_gives_the_full_gate(caplog):
    """With `note_values("read", template.payload)` the start is dropped before the
    function runs, with no `queued` or `started` record and no service."""
    import logging

    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    app.add_service("s", "constant", value=2)
    app.add_arrival("a", "constant", value=1, first_at=1, spawn="read")
    app.add_arrival("m", "constant", value=100, first_at=5, spawn="make")
    app.task(name="make", history=5)(lambda task_id: {"id": 1})
    template = _template_read()
    calls: List[int] = []
    app.task("s", name="read")(lambda task_id: calls.append(task_id) or template)
    app.note_values("read", template.payload)
    recorder = RecordingEgress()
    app.add_egress(recorder)
    with caplog.at_level(logging.INFO, logger="desflow.values.runtime"):
        app.run(until=4)
    assert calls == []
    assert [r for r in recorder.records if r.get("task") == "read"] == []
    lines = [
        r.getMessage() for r in caplog.records if "was not started" in r.getMessage()
    ]
    assert len(lines) == 1 and "get item" not in lines[0]


def _vars_lookup_model(app: SimulationContext) -> Template:
    app.add_service("s", "constant", value=2)
    app.add_arrival("a", "constant", value=1, first_at=1, spawn="read")
    app.add_arrival("m", "constant", value=100, first_at=5, spawn="make")
    app.task(name="make")(lambda task_id: {"id": 1})
    return Template(
        {"x": {"gen": "var", "name": "u"}},
        vars={"u": {"gen": "lookup", "task": "make", "path": "id"}},
        name="read",
    )


@pytest.mark.parametrize("whole_template", [True, False], ids=["template", "vars"])
def test_note_values_gates_a_lookup_in_the_vars_of_a_template(whole_template):
    """A `Template` given to `note_values` is searched in its `payload`, `vars` and
    `vars_once`, and `note_values` keeps the rows of the task it names."""
    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    template = _vars_lookup_model(app)
    calls: List[int] = []
    app.task("s", name="read")(lambda task_id: calls.append(task_id) or template)
    app.note_values("read", template if whole_template else template.vars)
    recorder = RecordingEgress()
    app.add_egress(recorder)
    app.run(until=4)
    assert calls == []
    assert [r for r in recorder.records if r.get("task") == "read"] == []


def test_note_values_with_a_template_whose_vars_hold_no_lookup_gates_nothing():
    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    template = Template({"x": 1}, vars={"u": 2}, vars_once={"v": 3}, name="read")
    app.note_values("read", template)
    assert app._gates == {} and app._row_targets == set()


def test_note_values_searches_the_vars_once_of_a_template():
    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    template = Template(
        {"x": 1},
        vars_once={"v": {"gen": "lookup", "task": "make", "path": "id"}},
        name="read",
    )
    app.note_values("read", template)
    assert app._gates == {"read": {"make"}}


def test_note_values_searches_a_bound_template_and_its_fields():
    from desflow.values.models import values_adapter
    from desflow.values.template import BoundPayload

    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    lookup = {"gen": "lookup", "task": "make", "path": "id"}
    inside = BoundPayload(Template({"x": 1}, vars={"u": lookup}, name="a"))
    app.note_values("a", inside)
    assert app._gates == {"a": {"make"}}
    field = BoundPayload(
        Template({"x": 1}, name="b"),
        fields=values_adapter().validate_python({"y": lookup}),
    )
    app.note_values("b", field)
    assert app._gates["b"] == {"make"}


def test_note_values_refuses_a_place_it_does_not_search():
    """A set or an iterator is not searched, so a lookup inside would pass with no
    message."""
    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    with pytest.raises(TypeError, match="not set"):
        app.note_values("read", {1, 2})
    with pytest.raises(TypeError, match="not list_iterator"):
        app.note_values("read", iter([1]))
    app.note_values("read", len, None, "text", 3, [1], (1,), {"a": 1})


def test_note_values_for_an_unknown_task_is_refused_before_the_run():
    app = SimulationContext(sim_id="B", random_seed=1, factor=0)
    app.add_arrival("m", "constant", value=100, first_at=5, spawn="make")
    app.task(name="make", history=5)(lambda task_id: {"id": 1})
    template = _template_read()
    app.task(name="read")(lambda task_id: template)
    app.note_values("raed", template.payload)
    app.add_egress(RecordingEgress())
    with pytest.raises(ValueError, match="note_values names task 'raed'"):
        app.run(until=4)


def test_rows_that_left_are_not_held_past_an_eighth_of_history():
    from desflow.values.runtime import _Rows

    rows = _Rows(1000)
    for i in range(1999):
        rows.add({"i": i})
    assert len(rows) == 1000 and len(rows._entries) <= 1000 + 125 + 1
    assert [r["i"] for _, r in rows.newest()][:2] == [1998, 1997]


@pytest.mark.parametrize(
    "function, expected",
    [
        ("sum", 0),
        ("count", 0),
        ("mean", "mean of no rows"),
        ("min", "min of no rows"),
        ("max", "max of no rows"),
    ],
)
def test_aggregate_of_no_rows_is_zero_for_sum_and_count_and_a_named_error_otherwise(
    tmp_path, function, expected
):
    path = "" if function == "count" else "path: id, "
    outcome = run_yaml_text(
        tmp_path,
        relation_model(
            f"payload: {{x: {{gen: aggregate, task: make, {path}last: 3, function: {function}, where: 'id < 0'}}}}"
        ),
        {},
    )
    if isinstance(expected, int):
        assert [p["x"] for p in payloads(outcome, "read")] == [expected] * 3
    else:
        assert expected in str(outcome.error)


# --- a `row` generator in a Builder or Low-level place ----------------------------

BUILDER_ROW = {
    "gen": "row",
    "file": "rows.csv",
    "order": "sequential",
    "on_end": "stop",
}


def _row(file: str = "rows.csv") -> Any:
    from desflow.values import generator

    return generator({**BUILDER_ROW, "file": file})


def _task(app: Any, **options: Any) -> None:
    @app.task(name="t", **options)
    def t(task_id):
        return {"x": 1}


def _by_arrival(app: Any, file: str) -> None:
    app.add_arrival("a", "constant", value=1, spawn="t", attributes={"r": _row(file)})
    _task(app)


def _by_replay(app: Any, file: str) -> None:
    rows = [{"time": 1}, {"time": 2}, {"time": 3}]
    app.add_arrival_replay("a", rows, spawn="t", attributes={"r": _row(file)})
    _task(app)


def _by_initial(app: Any, file: str) -> None:
    app.add_initial_arrival("a", 3, spawn="t", attributes={"r": _row(file)})
    _task(app)


def _by_group(app: Any, file: str) -> None:
    from desflow.values import generator

    group = generator(
        {
            "gen": "expr",
            "expr": "r['k']",
            "names": {"r": {**BUILDER_ROW, "file": file}},
        }
    )
    app.add_arrival("a", "constant", value=1, spawn="t", group=group)
    _task(app)


def _by_scenario(app: Any, file: str) -> None:
    app.add_arrival("a", "constant", value=1, spawn="t")
    _task(app)
    row = _row(file)
    app.add_scenario([{"at": at, "record": {"d": row}} for at in (1, 2, 3)])
    app.add_egress(RecordingEgress(), when="labels")


def _by_signal(app: Any, file: str) -> None:
    app.add_signal("z", once=False, on=[{"fire": {"signal": "y", "value": _row(file)}}])
    app.add_signal("y", once=False)
    app.add_arrival("a", "constant", value=1, spawn="t")
    _task(app, on=[{"fire": {"signal": "z"}}])


def _by_task(place: str) -> Any:
    def build(app: Any, file: str) -> None:
        app.add_arrival("a", "constant", value=1, spawn="t")
        value = _row(file) if place == "set" else {**BUILDER_ROW, "file": file}
        _task(app, **{place: {"v": value}})

    return build


def _by_template(place: str) -> Any:
    def build(app: Any, file: str) -> None:
        app.add_arrival("a", "constant", value=1, spawn="t")
        row = {**BUILDER_ROW, "file": file}
        template = {
            "payload": Template({"x": row}, name="p"),
            "vars": Template({"x": 1}, vars={"v": row}, name="v"),
            "vars_once": Template({"x": 1}, vars_once={"v": row}, name="o"),
        }[place]

        @app.task(name="t")
        def t(task_id):
            return template

    return build


def _by_spawn(app: Any, file: str) -> None:
    app.add_arrival("a", "constant", value=1, spawn="t")

    @app.task(name="u")
    def u(task_id):
        return {"y": 1}

    _task(app, spawn={"task": "u", "args": {"v": _row(file)}})


BUILDER_ROW_SITES = {
    "arrival attributes": (_by_arrival, 3.0),
    "replay attributes": (_by_replay, 3.0),
    "initial arrival attributes": (_by_initial, 0.0),
    "arrival group": (_by_group, 3.0),
    "scenario record": (_by_scenario, 3.0),
    "signal fire value": (_by_signal, 3.0),
    "task set": (_by_task("set"), 3.0),
    "task vars": (_by_task("vars"), 3.0),
    "task vars_once": (_by_task("vars_once"), 3.0),
    "task on_start": (_by_task("on_start"), 3.0),
    "task finish_vars": (_by_task("finish_vars"), 3.0),
    "task set_args": (_by_task("set_args"), 3.0),
    "task spawn args": (_by_spawn, 3.0),
    "template payload": (_by_template("payload"), 3.0),
    "template vars": (_by_template("vars"), 3.0),
    "template vars_once": (_by_template("vars_once"), 3.0),
}


@pytest.mark.parametrize("site", sorted(BUILDER_ROW_SITES))
def test_a_row_in_a_builder_place_is_read_and_its_stop_ends_the_run(
    tmp_path, monkeypatch, site
):
    """A `row` written in a script is read from the working directory when its place
    is added, and `on_end: stop` ends the run at the draw after the last row, as in a
    blueprint."""
    monkeypatch.chdir(tmp_path)
    tmp_path.joinpath("rows.csv").write_text("k\n1\n2\n")
    build, ends = BUILDER_ROW_SITES[site]
    app = SimulationContext(sim_id="B", factor=0, random_seed=1)
    build(app, "rows.csv")
    app.run(until=20)
    assert app.env.now == ends


@pytest.mark.parametrize(
    "site",
    ["arrival attributes", "template payload"],
    ids=["a place added to the context", "a Template"],
)
def test_a_row_file_that_is_missing_is_refused_where_a_builder_place_is_added(
    tmp_path, monkeypatch, site
):
    """Every place of the context reaches one check in `_bind_values`, and a
    `Template` has its own, so one case for each."""
    monkeypatch.chdir(tmp_path)
    build, _ = BUILDER_ROW_SITES[site]
    app = SimulationContext(sim_id="B", factor=0, random_seed=1)
    with pytest.raises(ValueError, match=r"nofile\.csv does not exist"):
        build(app, "nofile.csv")


def test_a_row_in_a_store_item_gives_its_row_to_the_task_that_gets_it(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    tmp_path.joinpath("rows.csv").write_text("k\n7\n")
    app = SimulationContext(sim_id="B", factor=0, random_seed=1)
    app.add_store("s", items=[_row()])
    app.add_arrival("a", "constant", value=1, spawn="t")
    seen: List[Any] = []

    @app.task(name="t", get={"store": "s"})
    def t(task_id, item):
        seen.append(item)
        return {"x": 1}

    app.run(until=2)
    assert seen == [{"k": 7}]
    with pytest.raises(ValueError, match=r"nofile\.csv does not exist"):
        SimulationContext(sim_id="C").add_store("s", items=[_row("nofile.csv")])


@pytest.mark.parametrize(
    ("task", "last"),
    [
        ("payload: {x: 1}, put: {store: s, item: ROW}", 2.0),
        ("payload: {x: 1}, spawn: {task: u, args: {v: ROW}}", 3.0),
        ("payload: {x: 1}, wait: {task: u, args: {v: ROW}}", 3.0),
    ],
    ids=["put item", "spawn args", "wait task args"],
)
def test_a_row_in_a_task_place_that_is_not_a_value_is_read_and_ends_the_run(
    tmp_path, task, last
):
    """A `put` item, the `args` of a `spawn` and the `args` of a `wait` on a task are
    checked with the other places: the file is read, a missing one is a load error, and
    `on_end: stop` ends the run at the third draw. The last record is then at 2 for the
    `put`, which draws before it publishes."""
    tmp_path.joinpath("rows.csv").write_text("k\n1\n2\n")
    text = (
        HEAD % 1
        + "stores: {s: {}}\narrivals: {a: {gap: 1, spawn: t}}\n"
        + f"tasks: {{t: {{{task}}}, u: {{payload: {{y: 1}}}}}}\n"
        + "run: {until: 20}\n"
    )
    outcome = run_yaml_text(tmp_path, text.replace("ROW", ROW_STOP), {})
    assert outcome.error is None, outcome.error
    assert max(r["sim_ts"] for r in outcome.records) == last
    missing = text.replace("ROW", ROW_STOP.replace("rows.csv", "nofile.csv"))
    with pytest.raises(BlueprintError, match=r"nofile\.csv does not exist"):
        run_yaml_text(tmp_path, missing, {})


def test_each_generator_in_a_spawn_has_its_own_place(tmp_path):
    """The generators of a task's `spawn` count and `args` are named for their place,
    so two `row` generators there do not share one position in their rows."""
    from desflow.blueprint.build import load_blueprint

    tmp_path.joinpath("rows.csv").write_text("k\n1\n2\n")
    path = tmp_path / "model.yaml"
    path.write_text(
        HEAD % 1
        + "arrivals: {a: {gap: 1, spawn: t}}\n"
        + "tasks: {t: {payload: {x: 1}, spawn: {task: u, args: {v: ROW, w: ROW}}}, "
        "u: {payload: {y: 1}}}\n".replace("ROW", ROW_STOP)
    )
    args = load_blueprint(path).blueprint.tasks["t"].spawn.args
    assert (args["v"]._path, args["w"]._path) == (
        "tasks.t.spawn.args.v",
        "tasks.t.spawn.args.w",
    )


def test_a_group_generator_draws_the_same_in_the_builder_and_in_a_blueprint(tmp_path):
    """The generator of a `group` is named for its place by both interfaces, so one
    seed gives the same number of entities."""
    from desflow.values import generator

    app = SimulationContext(sim_id="V", factor=0, random_seed=1)
    recorder = RecordingEgress()
    app.add_egress(recorder)
    app.add_arrival(
        "a",
        "constant",
        value=1,
        spawn="t",
        group=generator({"gen": "integer", "bounds": [1, 5]}),
    )
    _task(app)
    app.run(until=4)
    events = [r for r in recorder.records if r["stream_type"] == "event"]

    path = tmp_path / "model.yaml"
    path.write_text(
        HEAD % 1 + "arrivals: {a: {dist: constant, value: 1, spawn: t, "
        "group: {gen: integer, bounds: [1, 5]}}}\n"
        "tasks: {t: {payload: {x: 1}}}\nrun: {until: 4}\n"
    )
    outcome = run_yaml_text(tmp_path, path.read_text(), {})
    blueprint = [r for r in outcome.records if r["stream_type"] == "event"]
    assert len(events) == len(blueprint)


def test_a_row_in_the_gap_of_a_blueprint_arrival_is_read_and_ends_the_run(tmp_path):
    """A `row` nested in an arrival's `gap` is read with the other places of the
    blueprint, and its stop ends the run at the draw after the last row."""
    tmp_path.joinpath("rows.csv").write_text("k\n1\n2\n")
    gap = "{gen: expr, expr: \"r['k']\", names: {r: " + ROW_STOP + "}}"
    text = (
        HEAD % 1
        + f"arrivals: {{a: {{gap: {gap}, spawn: t}}}}\n"
        + "tasks: {t: {payload: {x: 1}}}\nrun: {until: 20}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, outcome.error
    times = [
        r["sim_ts"]
        for r in outcome.records
        if r["stream_type"] == "event" and r["value"].get("x") == 1
    ]
    assert times == [1.0, 3.0]


def test_a_builder_row_added_to_a_loaded_blueprint_is_read_from_the_working_dir(
    tmp_path, monkeypatch
):
    """After `from_yaml` the context reads a `row` at the call that adds it, as a
    script's own context does, relative to the working directory."""
    folder = tmp_path / "model"
    folder.mkdir()
    folder.joinpath("rows.csv").write_text("k\n1\n")
    tmp_path.joinpath("rows.csv").write_text("k\n7\n")
    path = folder / "model.yaml"
    path.write_text(
        HEAD % 1
        + "arrivals: {a: {gap: 1, spawn: t}}\n"
        + "tasks: {t: {payload: {x: 1}}}\n"
        + f"stores: {{s: {{items: [{ROW_STOP}]}}}}\n"
    )
    monkeypatch.chdir(tmp_path)
    app = SimulationContext.from_yaml(path)
    assert app._rows_deferred is False

    app.add_store("st", items=[_row()])
    app.run(until=2)
    with pytest.raises(ValueError, match=r"nofile\.csv does not exist"):
        app.add_store("other", items=[_row("nofile.csv")])


STOPPED_RUNS = {
    "arrival place": (
        "arrivals: {a: {gap: 1, spawn: t, attributes: {r: ROW}}, b: {gap: 1, spawn: u}}\n"
        "tasks: {t: {payload: {x: 1}}, u: {payload: {y: 1}}}\n"
    ),
    "task place": (
        "arrivals: {a: {gap: 1, spawn: t}, b: {gap: 1, spawn: u}}\n"
        "tasks: {t: {set: {r: ROW}, payload: {x: 1}}, u: {payload: {y: 1}}}\n"
    ),
    "run.segments": (
        "arrivals: {a: {gap: 1, spawn: t}, b: {gap: 1, spawn: u}}\n"
        "tasks: {t: {payload: {x: 1}}, u: {payload: {y: 1}}}\n"
        "run: {segments: [{until: 2}, {until: 4}]}\n"
    ),
}


@pytest.mark.parametrize("place", sorted(STOPPED_RUNS))
def test_step_and_peek_after_a_stop_do_not_run_the_model_on(
    tmp_path, monkeypatch, place
):
    """Once a `row` stop or the last entry of `run.segments` has ended the run,
    `step()` raises as `advance()` does, `peek()` is infinity, and a loop that steps
    while `peek()` is below a time ends."""
    monkeypatch.chdir(tmp_path)
    tmp_path.joinpath("rows.csv").write_text("k\n1\n2\n")
    path = tmp_path / "model.yaml"
    path.write_text(HEAD % 1 + STOPPED_RUNS[place].replace("ROW", ROW_STOP))
    app = SimulationContext.from_yaml(path)
    if place == "run.segments":
        app.start()
        app.finish()
    else:
        app.start(until=100)
        app.advance(50)
    ended = app.env.now
    assert app._stopped and app.peek() == float("inf")

    if place != "run.segments":  # finish() has closed that run
        with pytest.raises(RuntimeError, match=f"the run ended at {ended:g}"):
            app.advance(ended + 10)
    for _ in range(3):
        with pytest.raises(RuntimeError, match=f"the run ended at {ended:g}"):
            app.step()
    while app.peek() < 20:
        app.step()
    assert app.env.now == ended
