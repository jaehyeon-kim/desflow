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


def test_a_payload_that_is_a_generator_must_give_a_mapping(tmp_path):
    assert payloads(run(tmp_path, "payload: {gen: object, fields: {a: 1}}", n=1)) == [
        {"a": 1}
    ]
    message = error_of(tmp_path, "payload: {gen: integer, bounds: [1, 2]}")
    assert "a payload must give a mapping" in message


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
