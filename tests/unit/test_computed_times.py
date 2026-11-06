"""Computed times and arrivals: `service_time`, a computed wait timeout, computed `next`
weights, an arrival's `gap`, `first_at`, `count`, `keys`, `key`, `vars`, `stagger` and
`max_events`, a task `key`, `{gen: entity}` and `payload: null`."""

import logging
import re
import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import orjson
import pytest
from typer.testing import CliRunner

from desflow import SimulationContext, replicate
from desflow.blueprint import BlueprintError
from desflow.cli import app as cli
from tests.conformance.harness import run_yaml_text

HEAD = "simulation: {sim_id: C, factor: 0, random_seed: 3}\n"


def run(tmp_path: Path, body: str, arrivals: Any = None) -> Any:
    outcome = run_yaml_text(tmp_path, HEAD + textwrap.dedent(body), arrivals or {})
    return outcome


def events(outcome: Any, task: Optional[str] = None) -> List[Tuple[Any, ...]]:
    return [
        (round(r["sim_ts"], 3), r["key"], r["task"], r["value"])
        for r in outcome.records
        if r["stream_type"] == "event" and (task is None or r["task"] == task)
    ]


def done(outcome: Any, task: Optional[str]) -> List[Tuple[Any, ...]]:
    """The finished events of a task, not its status records."""
    return [
        (t, key, name, value)
        for t, key, name, value in events(outcome, task)
        if "status" not in value
    ]


def starts(outcome: Any, task: str = "t") -> List[Tuple[float, str]]:
    return [
        (t, key)
        for t, key, name, value in events(outcome, task)
        if value.get("event_type") == "ran"
    ]


def refused(tmp_path: Path, body: str, fragment: str) -> None:
    with pytest.raises(BlueprintError, match=re.escape(fragment)):
        run_yaml_text(tmp_path, HEAD + textwrap.dedent(body), {})


TASK = "tasks:\n  t: {payload: {event_type: ran}}\n"


# --- arrivals ----------------------------------------------------------------------


def test_a_gap_first_at_and_max_events(tmp_path):
    outcome = run(
        tmp_path,
        "arrivals: {a: {gap: 2, first_at: 5, max_events: 3, spawn: t}}\n"
        + TASK
        + "run: {until: 50}\n",
    )
    assert outcome.error is None
    assert starts(outcome) == [(5, "a-1"), (7, "a-2"), (9, "a-3")]


def test_a_first_at_in_the_past_arrives_at_once(tmp_path):
    outcome = run(
        tmp_path,
        "arrivals: {a: {gap: 2, first_at: 0, max_events: 2, spawn: t}}\n"
        + TASK
        + "run: {until: 50}\n",
    )
    assert starts(outcome) == [(0, "a-1"), (2, "a-2")]


def test_first_at_with_a_distribution(tmp_path):
    outcome = run(
        tmp_path,
        "arrivals: {a: {dist: constant, value: 3, first_at: 1, max_events: 2, "
        "spawn: t}}\n" + TASK + "run: {until: 50}\n",
    )
    assert starts(outcome) == [(1, "a-1"), (4, "a-2")]


def test_a_gap_text_that_is_a_duration_is_seconds(tmp_path):
    outcome = run(
        tmp_path,
        "arrivals: {a: {gap: '2 min', max_events: 2, spawn: t}}\n"
        + TASK
        + "run: {until: 500}\n",
    )
    assert starts(outcome) == [(120, "a-1"), (240, "a-2")]


def test_a_gap_can_read_the_registry_and_the_run(tmp_path):
    outcome = run(
        tmp_path,
        """\
        variables: {pace: 2}
        arrivals: {a: {gap: "registry['C.variables.pace'] + 1", max_events: 2, spawn: t}}
        tasks:
          t: {payload: {event_type: ran}}
        run: {until: 50}
        """,
    )
    assert starts(outcome) == [(3, "a-1"), (6, "a-2")]


def test_count_makes_an_entity_loop_for_each_and_stagger_spaces_them(tmp_path):
    outcome = run(
        tmp_path,
        "arrivals: {a: {gap: 5, count: 3, stagger: 1, max_events: 2, spawn: t}}\n"
        + TASK
        + "run: {until: 50}\n",
    )
    assert starts(outcome) == [
        (5, "a-1"),
        (6, "a-2"),
        (7, "a-3"),
        (10, "a-1"),
        (11, "a-2"),
        (12, "a-3"),
    ]


def test_count_with_a_stagger_later_than_first_at(tmp_path):
    outcome = run(
        tmp_path,
        "arrivals: {a: {gap: 4, count: 3, stagger: 2, first_at: 1, max_events: 1, "
        "spawn: t}}\n" + TASK + "run: {until: 50}\n",
    )
    assert starts(outcome) == [(1, "a-1"), (2, "a-2"), (4, "a-3")]


def test_a_count_of_zero_starts_nothing(tmp_path):
    outcome = run(
        tmp_path,
        "arrivals: {a: {gap: 1, count: 0, spawn: t}}\n" + TASK + "run: {until: 5}\n",
    )
    assert outcome.error is None and starts(outcome) == []


def test_a_stagger_of_zero_starts_the_loops_in_one_step(tmp_path):
    outcome = run(
        tmp_path,
        "arrivals: {a: {gap: 1, count: 3, stagger: 0, max_events: 1, spawn: t}}\n"
        + TASK
        + "run: {until: 5}\n",
    )
    assert starts(outcome) == [(1, "a-1"), (1, "a-2"), (1, "a-3")]


def test_keys_name_the_entities_and_give_the_count(tmp_path):
    outcome = run(
        tmp_path,
        "arrivals: {a: {gap: 2, keys: [north, 7, south], max_events: 1, spawn: t}}\n"
        + TASK
        + "run: {until: 10}\n",
    )
    assert starts(outcome) == [(2, "north"), (2, "7"), (2, "south")]


def test_key_is_a_value_read_for_each_entity(tmp_path):
    outcome = run(
        tmp_path,
        """\
        arrivals:
          a:
            gap: 2
            count: 2
            max_events: 1
            key: {gen: sequence, start: 100}
            spawn: t
        tasks:
          t: {payload: {event_type: ran}}
        run: {until: 10}
        """,
    )
    assert starts(outcome) == [(2, "100"), (2, "101")]


def test_a_key_that_is_not_text_or_a_whole_number_fails_the_run(tmp_path):
    outcome = run(
        tmp_path,
        "arrivals: {a: {gap: 2, max_events: 1, key: {gen: expr, expr: '1.5'}, "
        "spawn: t}}\n" + TASK + "run: {until: 10}\n",
    )
    assert isinstance(outcome.error, ValueError)
    assert "key gave 1.5" in str(outcome.error)


def test_a_source_skips_a_start_whose_key_is_still_running(tmp_path, caplog):
    body = """\
        services: {long: {dist: constant, value: 10}}
        arrivals: {a: {gap: 1, max_events: 3, key: k, spawn: t}}
        tasks:
          t: {service: long, payload: {event_type: ran}}
        run: {until: 30}
        """
    with caplog.at_level(logging.WARNING, logger="desflow"):
        outcome = run(tmp_path, body)
    assert outcome.error is None
    assert starts(outcome) == [(11, "k")]
    skipped = [m for m in caplog.messages if "skipped a start" in m]
    assert len(skipped) == 1 and "'k'" in skipped[0]


def test_the_key_of_a_finished_entity_may_start_again(tmp_path):
    body = """\
        arrivals: {a: {gap: 1, max_events: 3, key: k, spawn: t}}
        tasks:
          t: {payload: {event_type: ran}}
        run: {until: 30}
        """
    outcome = run(tmp_path, body)
    assert starts(outcome) == [(1, "k"), (2, "k"), (3, "k")]


def test_vars_are_read_by_the_attributes_and_the_key(tmp_path):
    outcome = run(
        tmp_path,
        """\
        arrivals:
          a:
            gap: 1
            count: 2
            max_events: 1
            vars: {n: {gen: sequence, start: 10}, m: {gen: expr, expr: "n * 2"}}
            attributes: {double: {gen: expr, expr: "m"}}
            key: {gen: format, template: "e{n}-{m}"}
            spawn: t
        tasks:
          t: {attributes: [double], payload: {event_type: ran}}
        run: {until: 5}
        """,
    )
    assert outcome.error is None
    rows = [(k, v["double"]) for _, k, _, v in events(outcome, "t")]
    assert rows == [("e10-20", 20), ("e11-22", 22)]


def test_the_entity_generator_gives_the_key_in_a_task(tmp_path):
    outcome = run(
        tmp_path,
        """\
        arrivals: {a: {gap: 1, keys: [x, y], max_events: 1, spawn: t}}
        tasks:
          t: {payload: {who: {gen: entity}}}
        run: {until: 5}
        """,
    )
    assert [v["who"] for _, _, _, v in events(outcome, "t")] == ["x", "y"]


def test_a_counted_entity_keeps_its_attributes_between_runs(tmp_path):
    outcome = run(
        tmp_path,
        """\
        arrivals:
          a:
            gap: 1
            count: 2
            max_events: 2
            attributes: {draw: {gen: sequence}}
            spawn: t
        tasks:
          t: {attributes: [draw], payload: {event_type: ran}}
        run: {until: 5}
        """,
    )
    rows = [(t, k, v["draw"]) for t, k, _, v in events(outcome, "t")]
    assert rows == [(1, "a-1", 0), (1, "a-2", 1), (2, "a-1", 0), (2, "a-2", 1)]


def test_a_counted_entity_holds_its_key_until_its_flow_ends(tmp_path, caplog):
    body = """\
        arrivals:
          a: {gap: 1, count: 3, stagger: 5, max_events: 1, key: same, spawn: t}
        tasks:
          t: {service_time: 100, payload: {event_type: ran}}
        run: {until: 30}
        """
    with caplog.at_level(logging.WARNING, logger="desflow"):
        outcome = run(tmp_path, body)
    assert outcome.error is None
    began = [
        (t, k) for t, k, _, v in events(outcome, "t") if v.get("status") == "started"
    ]
    assert began == [(1, "same")]
    assert len([m for m in caplog.messages if "skipped a start" in m]) == 1


def test_a_counted_entity_forgets_its_once_values_when_its_flow_ends(tmp_path):
    body = """\
        arrivals:
          a: {gap: 1, count: 2, stagger: 10, max_events: 1, key: same, spawn: t}
        tasks:
          t:
            service_time: 3
            vars_once: {n: {gen: sequence}}
            payload: {n: {gen: var, name: n}}
        run: {until: 30}
        """
    outcome = run(tmp_path, body)
    assert outcome.error is None
    assert [v["n"] for _, _, _, v in done(outcome, "t")] == [0, 1]


def test_a_counted_entity_keeps_its_once_values_while_a_run_is_in_the_model(tmp_path):
    body = """\
        arrivals:
          a: {gap: 1, count: 1, max_events: 3, key: same, spawn: t}
        tasks:
          t: {payload: {step: first}, next: u}
          u:
            service_time: 100
            vars_once: {m: {gen: sequence}}
            payload: {m: {gen: var, name: m}}
        run: {until: 200}
        """
    outcome = run(tmp_path, body)
    assert outcome.error is None
    assert [v["m"] for _, _, _, v in done(outcome, "u")] == [0, 0, 0]


def test_a_drain_ends_every_entity_loop_and_waits_for_the_running_flows(tmp_path):
    outcome = run(
        tmp_path,
        """\
        services: {long: {dist: constant, value: 4}}
        arrivals: {a: {gap: 1, count: 3, spawn: t}}
        tasks:
          t: {service: long, payload: {event_type: ran}}
        run: {until: 5, drain: true}
        """,
    )
    assert outcome.error is None
    assert sorted({t for t, _, _, _ in done(outcome, "t")}) == [5, 6, 7, 8]
    started = [t for t, _, _, v in events(outcome, "t") if v.get("status") == "started"]
    assert sorted(set(started)) == [1, 2, 3, 4] and len(started) == 12


def test_a_max_events_expression_is_read_once_at_the_start(tmp_path):
    outcome = run(
        tmp_path,
        "variables: {n: 2}\n"
        "arrivals: {a: {gap: 1, max_events: \"registry['C.variables.n']\", spawn: t}}\n"
        + TASK
        + "run: {until: 20}\n",
    )
    assert starts(outcome) == [(1, "a-1"), (2, "a-2")]


@pytest.mark.parametrize(
    "arrival, fragment",
    [
        ("{spawn: t}", "exactly one of dist, replay or gap"),
        (
            "{dist: constant, value: 1, gap: 2, spawn: t}",
            "exactly one of dist, replay or gap, not dist and gap",
        ),
        ("{gap: 1, keys: [a], count: 2, spawn: t}", "so count is not needed"),
        ("{gap: 1, keys: [a, a], spawn: t}", "lists each key once"),
        ("{gap: 1, keys: [], spawn: t}", "keys needs at least one key"),
        ("{gap: -1, spawn: t}", "gap must be 0 or more"),
        ("{gap: 1, stagger: -2, count: 2, spawn: t}", "stagger must be 0 or more"),
        ("{gap: 1, first_at: -1, spawn: t}", "first_at must be 0 or more"),
        ("{gap: 1, max_events: -1, spawn: t}", "max_events must be 0 or more"),
        ("{gap: 1, count: 2.5, spawn: t}", "count"),
        ("{gap: 1, count: true, spawn: t}", "count"),
        ("{gap: 1, max_events: 2.0, spawn: t}", "max_events"),
        ("{gap: 1, count: 'attr.x', spawn: t}", "this place reads the run"),
        (
            "{gap: 1, stagger: 'has(attr.x)', count: 2, spawn: t}",
            "this place reads the run",
        ),
        ("{gap: 1, max_events: 'attr.x', spawn: t}", "this place reads the run"),
        (
            "{gap: 1, vars: {n: {gen: attr, name: x}}, spawn: t}",
            "this place reads the run",
        ),
        (
            "{gap: 1, key: {gen: attr, name: x}, spawn: t}",
            "this place reads the run",
        ),
        ("{gap: 'nonsense +', spawn: t}", "gap"),
        ("{gap: 1, vars: {'1x': 2}, spawn: t}", "is not a name an expression can read"),
        ("{gap: 1, vars: {t: 2}, spawn: t}", "a name an expression already uses"),
        ("{gap: {gen: entity}, spawn: t}", "there is no task here"),
        (
            "{replay: {path: r.csv}, count: 2, spawn: t}",
            "count does not apply to a replay",
        ),
        (
            "{replay: {path: r.csv}, keys: [a], spawn: t}",
            "keys does not apply to a replay",
        ),
        (
            "{replay: {path: r.csv}, first_at: 1, spawn: t}",
            "first_at does not apply to a replay",
        ),
        (
            "{replay: {path: r.csv}, stagger: 1, spawn: t}",
            "stagger does not apply to a replay",
        ),
        (
            "{replay: {path: r.csv}, gap: 1, spawn: t}",
            "not replay and gap",
        ),
    ],
)
def test_arrival_keys_with_bad_input_name_the_rule(tmp_path, arrival, fragment):
    (tmp_path / "r.csv").write_text("time\n1\n")
    refused(tmp_path, f"arrivals: {{a: {arrival}}}\n" + TASK, fragment)


@pytest.mark.parametrize(
    "arrival, fragment",
    [
        ("{gap: 'x + 1', spawn: t}", "reads the name x"),
        ("{gap: 1, stagger: 'x', count: 2, spawn: t}", "reads the name x"),
        ("{gap: 1, count: 'x', spawn: t}", "reads the name x"),
        ("{gap: 1, max_events: 'x', spawn: t}", "reads the name x"),
        ("{gap: 'payload.x', spawn: t}", "reads payload"),
        ("{gap: 'process.name', spawn: t}", "reads process"),
        ("{gap: 'wait.a.value', spawn: t}", "reads wait"),
        ("{gap: 'item.x', spawn: t}", "reads item"),
        ("{gap: 'level.nope', spawn: t}", "level.nope names no store"),
        ("{gap: 'signal.nope.count', spawn: t}", "signal.nope names no signal"),
        ("{gap: 'users.nope', spawn: t}", "users.nope names no resource"),
        ("{gap: 'queue.nope', spawn: t}", "queue.nope names no resource"),
        ("{gap: 'putters.nope', spawn: t}", "putters.nope names no store"),
        ("{gap: 1, count: 'count(nope)', spawn: t}", "count(nope) names no store"),
        ("{gap: 1, max_events: 'level.nope', spawn: t}", "level.nope names no store"),
        (
            "{gap: 1, stagger: 'level.nope', count: 2, spawn: t}",
            "level.nope names no store",
        ),
        ("{gap: \"registry['nope.x']\", spawn: t}", "is not a registry path"),
        ("{gap: {gen: expr, expr: x}, spawn: t}", "is not a var that has a value"),
        ("{gap: {gen: expr, expr: payload.x}, spawn: t}", "reads payload"),
        ("{gap: {gen: expr, expr: level.nope}, spawn: t}", "level.nope names no store"),
        (
            "{gap: 1, key: {gen: expr, expr: payload.x}, spawn: t}",
            "reads payload",
        ),
        (
            "{gap: 1, key: {gen: when, condition: 'level.nope > 1', then: a, else: b},"
            " spawn: t}",
            "level.nope names no store",
        ),
        (
            "{gap: 1, vars: {n: {gen: expr, expr: 'process.name'}}, spawn: t}",
            "reads process",
        ),
        (
            "{gap: 1, vars: {n: {gen: expr, expr: 'signal.nope.count'}}, spawn: t}",
            "signal.nope names no signal",
        ),
    ],
)
def test_arrival_run_places_name_the_file_and_line(tmp_path, arrival, fragment):
    with pytest.raises(BlueprintError) as raised:
        run_yaml_text(tmp_path, HEAD + f"arrivals: {{a: {arrival}}}\n" + TASK, {})
    assert fragment in str(raised.value)
    assert re.search(r"model\.yaml:\d+", str(raised.value))


@pytest.mark.parametrize(
    "arrival, fragment",
    [
        ("{gap: 1, count: 2, spawn: t}", None),
        ("{gap: 1, count: 2}", "count sets how an arrival starts its task"),
        ("{gap: 1, keys: [a, b]}", "keys sets how an arrival starts its task"),
        ("{gap: 1, max_events: 2}", "max_events sets how an arrival starts"),
        ("{gap: 1, stagger: 1}", "stagger sets how an arrival starts"),
        (
            "{gap: 1, stagger: 1, spawn: t}",
            "stagger is the time between the starts of two entities",
        ),
        ("{gap: true, spawn: t}", "gap must be a number of seconds"),
        ("{gap: 1, stagger: true, count: 2, spawn: t}", "stagger must be a number"),
        ("{gap: '-1', spawn: t}", "gap must be 0 or more"),
        ("{gap: 1, stagger: '-1', count: 2, spawn: t}", "stagger must be 0 or more"),
    ],
)
def test_arrival_keys_that_would_be_ignored_are_refused(tmp_path, arrival, fragment):
    text = f"arrivals: {{a: {arrival}}}\n" + TASK + "run: {until: 3}\n"
    if fragment is None:
        assert run(tmp_path, text).error is None
    else:
        refused(tmp_path, text, fragment)


def test_an_arrival_without_spawn_keeps_gap_first_at_key_and_vars(tmp_path):
    # `wait_for_arrival` reads these four, so they stay allowed without spawn
    text = "arrivals: {a: {gap: 1, first_at: 2, key: k, vars: {n: 3}}}\n" + TASK
    assert run(tmp_path, text + "run: {until: 3}\n").error is None


def test_a_replay_takes_max_events_key_and_vars(tmp_path):
    outcome = run(
        tmp_path,
        """\
        arrivals:
          a: {replay: {path: a.csv}, max_events: 2, key: {gen: sequence, start: 50}, spawn: t}
        tasks:
          t: {payload: {event_type: ran}}
        run: {until: 20}
        """,
        {"a": [1, 2, 3, 4]},
    )
    assert starts(outcome) == [(1, "50"), (2, "51")]


# --- tasks -------------------------------------------------------------------------


def test_service_time_replaces_a_service(tmp_path):
    outcome = run(
        tmp_path,
        """\
        resources: {m: {current_cap: 1}}
        arrivals: {a: {gap: 1, max_events: 2, attributes: {size: 3}, spawn: t}}
        tasks:
          t: {resource: m, service_time: "attr.size * 2", payload: {event_type: ran}}
        run: {until: 30}
        """,
    )
    assert outcome.error is None
    assert starts(outcome) == [(7, "a-1"), (13, "a-2")]


def test_service_time_as_a_duration_and_a_generator(tmp_path):
    outcome = run(
        tmp_path,
        """\
        resources: {m: {current_cap: 2}}
        arrivals: {a: {gap: 1, max_events: 1, spawn: x}}
        tasks:
          x:
            resource: m
            service_time: {gen: literal, value: 4}
            payload: {event_type: ran}
            next: y
          y: {resource: m, service_time: "1 min", payload: {event_type: ran}}
        run: {until: 100}
        """,
    )
    assert outcome.error is None
    assert [(t, task) for t, _, task, _ in done(outcome, None)] == [(5, "x"), (65, "y")]


def test_service_time_reading_a_var_draws_it_once_for_the_payload(tmp_path):
    outcome = run(
        tmp_path,
        """\
        arrivals: {a: {gap: 1, max_events: 3, spawn: t}}
        tasks:
          t:
            vars: {d: {gen: sequence, start: 1}}
            service_time: d
            payload: {event_type: ran, d: {gen: var, name: d}}
        run: {until: 30}
        """,
    )
    assert outcome.error is None
    assert [(t, v["d"]) for t, _, _, v in done(outcome, "t")] == [
        (2, 1),
        (4, 2),
        (6, 3),
    ]


def test_service_time_reads_the_item_a_get_took(tmp_path):
    outcome = run(
        tmp_path,
        """\
        stores: {s: {items: [{time: 3}, {time: 5}]}}
        arrivals: {a: {gap: 1, max_events: 2, spawn: t}}
        tasks:
          t:
            get: {store: s}
            service_time: "item.time"
            payload: {event_type: ran}
        run: {until: 30}
        """,
    )
    assert outcome.error is None
    assert [t for t, _, _, _ in done(outcome, "t")] == [4, 7]


def test_a_service_time_that_is_not_a_number_fails_the_run(tmp_path):
    outcome = run(
        tmp_path,
        """\
        arrivals: {a: {gap: 1, max_events: 1, attributes: {name: x}, spawn: t}}
        tasks:
          t: {service_time: "attr.name", payload: {event_type: ran}}
        run: {until: 5}
        """,
    )
    assert isinstance(outcome.error, TypeError)
    assert "service_time gave 'x'" in str(outcome.error)


@pytest.mark.parametrize(
    "task, fragment",
    [
        (
            "{service: s, service_time: 2, payload: {}}",
            "a service or a service_time, not both",
        ),
        (
            "{resource: m, payload: {}}",
            "a task with a resource needs a service or a service_time",
        ),
        ("{service_time: -1, payload: {}}", "service_time must be 0 or more"),
        ("{service_time: 'x +', payload: {}}", "service_time"),
        ("{service_time: 'zz + 1', payload: {}}", "zz"),
        ("{service_time: 'item.t', payload: {}}", "item.<field>"),
        (
            "{vars: {a: 1}, finish_vars: {f: 1}, service_time: f, payload: {}}",
            "finish_vars name",
        ),
        (
            "{service_time: {gen: var, name: nope}, vars: {a: 1}, payload: {}}",
            "nope",
        ),
        ("{key: k, key_field: f, payload: {f: 1}}", "key and key_field both"),
        ("{payload: null, key_field: f}", "payload: null gives no payload"),
        (
            "{payload: null, put: {store: q}}",
            "payload: null publishes no record",
        ),
        ("{payload: null, id_field: i}", "id_field applies only to a mapping"),
        (
            "{payload: null, next: {when: [{condition: 'payload.x > 1', task: t}]}}",
            "payload: null gives the task no payload",
        ),
        ("{payload: null, fail: 'payload.x > 1'}", "payload: null gives the task"),
    ],
)
def test_task_keys_with_bad_input_name_the_rule(tmp_path, task, fragment):
    refused(
        tmp_path,
        "resources: {m: {current_cap: 1}}\nservices: {s: {dist: constant, value: 1}}\n"
        "stores: {q: {}}\n"
        f"tasks:\n  t: {task}\n",
        fragment,
    )


def test_a_task_without_payload_is_still_required(tmp_path):
    refused(tmp_path, "tasks:\n  t: {service_time: 1}\n", "payload")


def test_a_task_key_is_the_record_key_of_each_instance(tmp_path):
    outcome = run(
        tmp_path,
        """\
        arrivals: {a: {gap: 1, max_events: 2, spawn: t}}
        tasks:
          t:
            key: {gen: sequence, start: 40}
            vars_once: {}
            payload: {event_type: ran}
        run: {until: 10}
        """,
    )
    assert outcome.error is None
    assert [k for _, k, _, _ in done(outcome, "t")] == ["40", "41"]


def test_a_task_key_that_is_not_text_or_a_whole_number_fails_the_run(tmp_path):
    outcome = run(
        tmp_path,
        """\
        arrivals: {a: {gap: 1, max_events: 1, spawn: t}}
        tasks:
          t: {key: {gen: expr, expr: "1.5"}, payload: {event_type: ran}}
        run: {until: 10}
        """,
    )
    assert isinstance(outcome.error, (ValueError, TypeError))
    assert "1.5" in str(outcome.error)


def test_payload_null_with_a_resource_still_publishes_the_statuses(tmp_path):
    outcome = run(
        tmp_path,
        """\
        resources: {m: {current_cap: 1}}
        arrivals: {a: {gap: 1, max_events: 1, spawn: t}}
        tasks:
          t: {resource: m, service_time: 2, payload: null}
        run: {until: 10}
        """,
    )
    assert [v["status"] for _, _, _, v in events(outcome, "t")] == [
        "queued",
        "started",
    ]


@pytest.mark.parametrize("n", [1, 3, 10])
def test_wait_timeout_text_and_generator_are_read_when_the_wait_starts(tmp_path, n):
    outcome = run(
        tmp_path,
        f"""\
        arrivals: {{a: {{gap: 1, max_events: {n}, attributes: {{w: 2}}, spawn: t}}}}
        tasks:
          t:
            wait: {{timeout: "attr.w * 2"}}
            payload: {{event_type: ran}}
          u: {{payload: {{}}}}
        run: {{until: 50}}
        """,
    )
    assert outcome.error is None
    assert [t for t, _, _, _ in done(outcome, "t")] == [i + 4 for i in range(1, n + 1)]


def test_wait_timeout_after_a_generator_with_a_value(tmp_path):
    outcome = run(
        tmp_path,
        """\
        arrivals: {a: {gap: 1, max_events: 1, spawn: t}}
        tasks:
          t:
            wait:
              timeout:
                after: {gen: literal, value: 3}
                value: {gen: expr, expr: "t"}
            payload: {event_type: ran, got: {gen: expr, expr: "wait.timeout.value"}}
        run: {until: 50}
        """,
    )
    assert outcome.error is None
    assert [(t, v["got"]) for t, _, _, v in done(outcome, "t")] == [(4, 1)]


def test_a_wait_timeout_generator_is_a_bare_value(tmp_path):
    outcome = run(
        tmp_path,
        """\
        arrivals: {a: {gap: 1, max_events: 1, spawn: t}}
        tasks:
          t: {wait: {timeout: {gen: uniform, low: 2, high: 2}}, payload: {event_type: ran}}
        run: {until: 50}
        """,
    )
    assert [t for t, _, _, _ in done(outcome, "t")] == [3]


@pytest.mark.parametrize(
    "timeout, fragment",
    [
        ("-1", "a wait timeout must be 0 or more"),
        ("'nonsense +'", "a wait timeout is seconds, a duration such as '5 min'"),
        ("'zz + 1'", "zz"),
        ("{after: -1}", "a wait timeout must be 0 or more"),
        ("{after: {gen: uniform, low: 1}}", "high"),
    ],
)
def test_wait_timeouts_with_bad_input_name_the_rule(tmp_path, timeout, fragment):
    refused(
        tmp_path,
        f"tasks:\n  t: {{wait: {{timeout: {timeout}}}, payload: {{}}}}\n",
        fragment,
    )


def test_wait_timeout_in_any_reads_an_expression(tmp_path):
    outcome = run(
        tmp_path,
        """\
        signals: {go: {}}
        arrivals: {a: {gap: 1, max_events: 1, spawn: t}}
        tasks:
          t:
            wait: {any: {go: {signal: go}, late: {timeout: "1 + 2"}}}
            payload: {event_type: ran, late: {gen: expr, expr: "wait.late.done"}}
        run: {until: 50}
        """,
    )
    assert [(t, v["late"]) for t, _, _, v in done(outcome, "t")] == [(4, True)]


# --- next.weights ------------------------------------------------------------------


def test_numeric_text_weights_load_as_numbers(tmp_path):
    app = SimulationContext.from_yaml(
        _write(
            tmp_path,
            HEAD
            + "arrivals: {a: {gap: 1, spawn: t}}\n"
            + "tasks:\n"
            + "  t: {payload: {}, next: {weights: {u: '0.5', v: 0.5}}}\n"
            + "  u: {payload: {}}\n"
            + "  v: {payload: {}}\n"
            + "run: {until: 5}\n",
        )
    )
    router = app._tasks.specs["t"].router
    assert router.computed is False
    assert router.probabilities == {"u": 0.5, "v": 0.5}


def test_expression_weights_are_read_when_the_task_finishes(tmp_path):
    outcome = run(
        tmp_path,
        """\
        arrivals:
          a: {gap: 1, count: 6, max_events: 1, attributes: {p: {gen: sequence, step: 1}}, spawn: t}
        tasks:
          t: {payload: {}, next: {weights: {u: "attr.p // 3", v: "1 - attr.p // 3"}}}
          u: {payload: {event_type: u}}
          v: {payload: {event_type: v}}
        run: {until: 5}
        """,
    )
    assert outcome.error is None
    routed = {k: task for _, k, task, v in events(outcome) if task in ("u", "v")}
    assert routed == {
        "a-1": "v",
        "a-2": "v",
        "a-3": "v",
        "a-4": "u",
        "a-5": "u",
        "a-6": "u",
    }


@pytest.mark.parametrize(
    "weights, fragment",
    [
        ("{u: 0, v: 0}", "weights must add up to more than 0"),
        ("{u: '0 - 1', v: '0 - 1'}", "must be 0 or more"),
        ("{u: 2, v: '0 - 1'}", "the weight of 'v' must be 0 or more"),
        ("{u: 'attr.name', v: 1}", "must be a number"),
    ],
)
def test_weights_that_fail_when_read_stop_the_run(tmp_path, weights, fragment):
    outcome = (
        run(
            tmp_path,
            f"""\
        arrivals: {{a: {{gap: 1, max_events: 1, attributes: {{name: x}}, spawn: t}}}}
        tasks:
          t: {{payload: {{}}, next: {{weights: {weights}}}}}
          u: {{payload: {{}}}}
          v: {{payload: {{}}}}
        run: {{until: 5}}
        """,
        )
        if not weights.startswith("{u: 0, v: 0}")
        else None
    )
    if outcome is None:
        refused(
            tmp_path,
            "tasks:\n  t: {payload: {}, next: {weights: {u: 0, v: 0}}}\n"
            "  u: {payload: {}}\n  v: {payload: {}}\n",
            fragment,
        )
        return
    assert isinstance(outcome.error, ValueError)
    assert fragment in str(outcome.error)


def test_an_expression_weight_that_reads_an_unknown_name_is_refused(tmp_path):
    refused(
        tmp_path,
        "tasks:\n  t: {payload: {}, next: {weights: {u: 'zz + 1', v: 1}}}\n"
        "  u: {payload: {}}\n  v: {payload: {}}\n",
        "zz",
    )


def test_weights_expressions_draw_once_from_the_route_stream(tmp_path):
    def routes(weights: str) -> List[str]:
        outcome = run(
            tmp_path,
            f"""\
            arrivals: {{a: {{gap: 1, max_events: 12, spawn: t}}}}
            tasks:
              t: {{payload: {{}}, next: {{weights: {weights}}}}}
              u: {{payload: {{event_type: u}}}}
              v: {{payload: {{event_type: v}}}}
            run: {{until: 50}}
            """,
        )
        return [task for _, _, task, _ in events(outcome) if task in ("u", "v")]

    assert routes("{u: 0.25, v: 0.75}") == routes("{u: '0.5 / 2', v: '1 - 0.25'}")


def test_a_patience_that_loaded_before_still_loads(tmp_path):
    # A negative or nan literal on a task nothing reaches, and a nan on one that runs
    text = """\
        arrivals: {a: {dist: constant, value: 1, max_events: 1, spawn: t}}
        tasks:
          t: {wait: {timeout: 5}, patience: .nan, payload: {event_type: ran}}
          never: {wait: {timeout: 5}, patience: -1, payload: {}}
        run: {until: 5}
        """
    assert run(tmp_path, text).error is None


def test_a_negative_patience_expression_names_the_task(tmp_path):
    text = """\
        arrivals: {a: {dist: constant, value: 1, max_events: 1, attributes: {n: -2}, spawn: t}}
        tasks:
          t: {wait: {timeout: 5}, patience: attr.n, payload: {event_type: ran}}
        run: {until: 5}
        """
    error = run(tmp_path, text).error
    assert isinstance(error, ValueError)
    assert "task 't' patience gave -2" in str(error)


@pytest.mark.parametrize("timeout", ["'-1'", "{after: '-1', value: 1}", "-1"])
def test_a_negative_timeout_in_text_is_refused_at_load(tmp_path, timeout):
    text = f"""\
        tasks:
          t: {{wait: {{timeout: {timeout}}}, payload: {{}}}}
        """
    refused(tmp_path, text, "a wait timeout must be 0 or more")


def test_a_timeout_in_text_that_is_a_number_waits_that_long(tmp_path):
    text = """\
        arrivals: {a: {gap: 1, max_events: 1, spawn: t}}
        tasks:
          t: {wait: {timeout: '2.5'}, payload: {event_type: ran}}
        run: {until: 10}
        """
    assert starts(run(tmp_path, text)) == [(3.5, "a-1")]


def test_a_task_refuses_key_with_key_field():
    app = SimulationContext(sim_id="C", factor=0)
    with pytest.raises(ValueError, match="key and key_field both set"):

        @app.task(key="k", key_field="id")
        def t(task_id):
            return {"id": 1}


def test_a_second_add_arrival_drops_the_first_ones_first_at():
    app = SimulationContext(sim_id="C", factor=0)
    app.add_arrival("a", dist="constant", value=3, first_at=7)
    app.add_arrival("a", dist="constant", value=3)
    seen: List[float] = []

    @app.arrival_loop("a")
    def loop(context):
        yield context.wait_for_arrival("a")
        seen.append(context.env.now)

    app.run(until=20)
    assert seen == [3]


# --- builder and low level ---------------------------------------------------------


def test_the_builder_takes_numpy_numbers_for_times():
    app = SimulationContext(sim_id="C", factor=0)
    app.add_resource("m", current_cap=1)
    app.add_arrival("a", dist="constant", value=1, first_at=np.float64(2))

    @app.task(resource_id="m", service_time=np.float64(1.5))
    def t(task_id):
        return {"event_type": "ran"}

    assert app._tasks.specs["t"].service_time == 1.5


@pytest.mark.parametrize("bad", [-1, True, "x +", float("nan")])
def test_add_arrival_refuses_a_bad_first_at(bad):
    app = SimulationContext(sim_id="C", factor=0)
    with pytest.raises(ValueError, match="first_at"):
        app.add_arrival("a", dist="constant", value=1, first_at=bad)


def test_the_builder_first_at_delays_the_first_arrival():
    app = SimulationContext(sim_id="C", factor=0)
    app.add_arrival("a", dist="constant", value=3, first_at=7)
    seen: List[float] = []

    @app.arrival_loop("a")
    def loop(context):
        for _ in range(2):
            yield context.wait_for_arrival("a")
            seen.append(context.env.now)

    app.run(until=20)
    assert seen == [7, 10]


def test_the_builder_task_key(tmp_path):
    app = SimulationContext(sim_id="C", factor=0)
    app.add_arrival("a", dist="constant", value=1, spawn="t")

    @app.task(key="fixed")
    def t(task_id):
        return {"event_type": "ran"}

    captured: List[Dict[str, Any]] = []
    from desflow.connectors.egress.base import BaseEgress

    class Keep(BaseEgress):
        async def run(self, egress_queue):
            import asyncio
            import queue

            while True:
                try:
                    captured.extend(egress_queue.get_nowait())
                except queue.Empty:
                    await asyncio.sleep(0.01)

    app.add_egress(Keep())
    app.run(until=3)
    assert {r["key"] for r in captured if r["stream_type"] == "event"} == {"fixed"}


# --- replicate and --sample --------------------------------------------------------


COUNTED = """\
simulation: {sim_id: C, factor: 0, random_seed: 3}
arrivals: {a: {gap: {gen: uniform, low: 1, high: 3}, count: 3, stagger: 0.5, spawn: t}}
resources: {m: {current_cap: 1}}
services: {s: {dist: constant, value: 0.2}}
tasks:
  t:
    resource: m
    service_time: {gen: uniform, low: 0.1, high: 0.4}
    payload: {event_type: ran}
stats: typical
run: {until: 200}
"""


def test_replicate_runs_a_blueprint_with_counted_arrivals(tmp_path):
    path = _write(tmp_path, COUNTED)
    first = replicate(path, 3, seed=5, until=200, measures=["tasks.t.wait_mean"])
    again = replicate(path, 3, seed=5, until=200, measures=["tasks.t.wait_mean"])
    assert first.values == again.values
    assert len({tuple(v) for v in first.values.values()}) >= 1


def test_sample_prints_events_of_a_counted_blueprint(tmp_path):
    path = _write(tmp_path, COUNTED)
    result = CliRunner().invoke(cli, ["run", str(path), "--sample", "6"])
    assert result.exit_code == 0, result.output
    lines = [orjson.loads(line) for line in result.stdout.splitlines()]
    assert len([r for r in lines if r["stream_type"] == "event"]) == 6


def test_a_run_with_the_same_seed_repeats(tmp_path):
    text = COUNTED
    a = run_yaml_text(tmp_path, text, {})
    b = run_yaml_text(tmp_path, text, {})
    assert events(a) == events(b) and events(a)


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "model.yaml"
    path.write_text(textwrap.dedent(text), encoding="utf-8")
    return path
