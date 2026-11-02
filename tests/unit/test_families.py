"""Batch 11 families and references: a resource, store, container or signal made from a
key, in the places a name is accepted, and what a run leaves behind."""

import pytest

from desflow.core.context import SimulationContext
from tests.conformance.harness import run_yaml_text


def _done(outcome):
    """The payload records of a run as (time, key, payload), with no status record."""
    assert outcome.error is None, repr(outcome.error)
    return [
        (round(r["sim_ts"], 3), r["key"], r["value"])
        for r in outcome.records
        if r["stream_type"] == "event"
        and isinstance(r["value"], dict)
        and "status" not in r["value"]
    ]


def _statuses(outcome, status):
    return [
        (round(r["sim_ts"], 3), r["key"])
        for r in outcome.records
        if r["stream_type"] == "event"
        and isinstance(r["value"], dict)
        and r["value"].get("status") == status
    ]


def _rows(tmp_path, n, at=0):
    (tmp_path / "a.csv").write_text(
        "time,i\n" + "".join(f"{at},{i}\n" for i in range(n))
    )


HEAD = "simulation: {sim_id: S, factor: 0, random_seed: 42}\n"


def test_seize_and_release_take_a_reference(tmp_path):
    """Y1: a `seize` and a `release` name the member the key gives, so entities 0 and 2
    share desk 0 and entities 1 and 3 share desk 1."""
    _rows(tmp_path, 4)
    text = (
        HEAD
        + """
resources: {desk: {family: true, current_cap: 1}}
arrivals: {a: {replay: {path: a.csv}, spawn: first}}
tasks:
  first:
    seize: {resource: {of: desk, key: "attr.i % 2"}}
    service_time: 2
    payload: {e: held, i: {gen: attr, name: i}}
    next: last
  last:
    release: {of: desk, key: "attr.i % 2"}
    payload: {e: freed}
run: {until: 50}
"""
    )
    outcome = run_yaml_text(tmp_path, text, {})
    held = [(t, v["i"]) for t, _k, v in _done(outcome) if v["e"] == "held"]
    assert held == [(2.0, 0), (2.0, 1), (4.0, 2), (4.0, 3)]
    assert outcome.app_or_env._resources_map["desk[0]"].count == 0
    assert outcome.app_or_env._resources_map["desk[1]"].count == 0


def test_a_task_level_signal_names_a_member_and_a_stop_signal_ends_the_run(tmp_path):
    """Y1: the `signal:` a task fires when it ends may be a member, and `run.stop.signal`
    names a member the run waits for, with its key read once at the start."""
    _rows(tmp_path, 3)
    text = (
        HEAD
        + """
signals: {done: {family: true}}
arrivals: {a: {replay: {path: a.csv}, spawn: work}}
tasks:
  work:
    service_time: "attr.i + 1"
    signal: {of: done, key: attr.i}
    payload: {e: ended, i: {gen: attr, name: i}}
run: {until: 100, stop: {signal: {of: done, key: 1}}}
"""
    )
    outcome = run_yaml_text(tmp_path, text, {})
    ended = [(t, v["i"]) for t, _k, v in _done(outcome) if v["e"] == "ended"]
    assert ended == [(1.0, 0), (2.0, 1)]
    assert outcome.app_or_env.env.now == 2.0


def test_a_member_has_the_breakdown_of_its_family(tmp_path):
    """Y1: a member made at its first use starts the breakdown of its family, whose
    failure interrupts the unit's holder."""
    _rows(tmp_path, 1)
    text = (
        HEAD
        + """
resources:
  desk:
    family: true
    current_cap: 1
    breakdown: {time_to_failure: {dist: constant, value: 3}, repair: {dist: constant, value: 2}}
arrivals: {a: {replay: {path: a.csv}, spawn: job}}
tasks:
  job:
    resource: {of: desk, key: attr.i}
    service_time: 10
    payload: {e: done}
run: {until: 30}
"""
    )
    outcome = run_yaml_text(tmp_path, text, {})
    assert _statuses(outcome, "interrupted")[0] == (3.0, "a-1")


def test_a_second_run_of_a_context_starts_with_no_member_of_the_first(tmp_path):
    """Y1: members made by one run are removed when the next is built, so a read of a
    key the first run made gives what a new member gives."""
    _rows(tmp_path, 2)
    text = (
        HEAD
        + """
resources: {desk: {family: true, current_cap: 1}}
arrivals: {a: {replay: {path: a.csv}, spawn: job}}
tasks:
  job:
    resource: {of: desk, key: attr.i}
    service_time: 1
    payload: {e: done, x: {gen: expr, expr: "members(desk)"}}
run: {until: 20}
"""
    )
    outcome = run_yaml_text(tmp_path, text, {})
    first = _done(outcome)
    context = outcome.app_or_env
    assert [v["x"] for _t, _k, v in first] == [[0, 1], [0, 1]]
    assert context._families["desk"].keys == [0, 1]
    before = len(outcome.records)
    context.run()
    second = [r for r in outcome.records[before:] if r["stream_type"] == "event"]
    done = [r["value"]["x"] for r in second if "x" in r["value"]]
    assert done == [[0, 1], [0, 1]]
    assert context._families["desk"].keys == [0, 1]
    assert sorted(n for n in context._resources_map if n.startswith("desk[")) == [
        "desk[0]",
        "desk[1]",
    ]


def test_a_scenario_path_names_a_member_that_the_run_makes(tmp_path):
    """Y1: a registry path with a bracketed part is one part, and it loads when it
    names an attribute of a family's member that exists when the step is due."""
    _rows(tmp_path, 2)
    text = (
        HEAD
        + """
resources: {desk: {family: true, members: 1, current_cap: 1, max_cap: 3}}
arrivals: {a: {replay: {path: a.csv}, spawn: job}}
tasks:
  job:
    resource: {of: desk, key: 0}
    service_time: 5
    payload: {e: done, cap: {gen: expr, expr: "registry['S.resources.desk[0].current_cap']"}}
scenario:
  - {at: 1, path: "S.resources.desk[0].current_cap", value: 2}
run: {until: 30}
"""
    )
    outcome = run_yaml_text(tmp_path, text, {})
    assert [(t, k, v["cap"]) for t, k, v in _done(outcome)] == [
        (5.0, "a-1", 2),
        (6.0, "a-2", 2),
    ]


def test_a_list_of_names_picks_the_declared_name_the_key_gives(tmp_path):
    """Y2: `of: [left, right]` takes the name the key gives, and a name the list does
    not hold is a run failure that names it."""
    _rows(tmp_path, 2)
    text = (
        HEAD
        + """
resources: {left: {current_cap: 1}, right: {current_cap: 1}}
arrivals: {a: {replay: {path: a.csv}, spawn: job}}
tasks:
  job:
    resource: {of: [left, right], key: "['left', 'right', 'middle'][attr.i * 2]"}
    service_time: 1
    payload: {e: done}
run: {until: 20}
"""
    )
    outcome = run_yaml_text(tmp_path, text, {})
    assert isinstance(outcome.error, (KeyError, RuntimeError, ValueError)) or (
        outcome.error is not None
    )
    assert "middle" in str(outcome.error)


def test_a_key_that_is_not_an_integer_or_text_is_a_run_failure(tmp_path):
    _rows(tmp_path, 1)
    text = (
        HEAD
        + """
resources: {desk: {family: true, current_cap: 1}}
arrivals: {a: {replay: {path: a.csv}, spawn: job}}
tasks:
  job:
    resource: {of: desk, key: "attr.i + 0.5"}
    service_time: 1
    payload: {e: done}
run: {until: 20}
"""
    )
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is not None
    assert "an integer or text" in str(outcome.error)


def test_a_queued_each_request_is_cancelled_when_the_task_ends(tmp_path):
    """Fixes 2 and 10: a task that waits for `any` of two requests and finishes with the
    first leaves no request in the queue of the one it did not get."""
    _rows(tmp_path, 1)
    text = (
        HEAD
        + """
resources: {a: {current_cap: 1}, b: {current_cap: 1}}
arrivals:
  hold: {dist: constant, value: 1, spawn: holdb, max_events: 1, first_at: 0}
  w: {replay: {path: a.csv}, spawn: waiter}
tasks:
  holdb: {resource: b, service_time: 100, payload: {e: held}}
  waiter:
    wait: {each: ["a", "b"], group: any, branch: {request: {of: [a, b], key: "['a', 'b'][each.index]"}}}
    payload: {e: got}
run: {until: 20}
"""
    )
    outcome = run_yaml_text(tmp_path, text, {})
    context = outcome.app_or_env
    assert outcome.error is None, repr(outcome.error)
    assert len(context._resources_map["b"].queue) == 0
    assert len(context._resources_map["a"].queue) == 0


def test_an_order_of_one_to_five_items_with_a_total_needs_no_python(tmp_path):
    """DX-1: the example `postgres_orders.yaml` written with `wait: {each}`: the items
    are published first, one for each line, then the order with the total of its lines,
    as the generator in `postgres_orders_logic.py` publishes them. The tables are left
    out, because they need a database."""
    text = (
        HEAD
        + """
arrivals: {orders: {dist: constant, value: 1, spawn: order, max_events: 60, attributes: {order_id: {gen: sequence, start: 1}}}}
tasks:
  order:
    set: {lines: {gen: repeat, times: {gen: integer, bounds: [1, 5]}, value: {price: {gen: uniform, low: 10.0, high: 50.0, decimals: 2}, quantity: {gen: integer, bounds: [1, 3]}}}}
    payload: {event_type: drawn}
    next: items
  items:
    wait: {each: "attr.lines", branch: {task: item, args: {line: {gen: expr, expr: each.value}}}}
    payload: {t: orders, order_id: {gen: attr, name: order_id}, total_amount: {gen: expr, expr: "sum([l.price * l.quantity for l in attr.lines])", decimals: 2}, status: pending}
  item:
    payload: {t: order_items, order_item_id: {gen: sequence, start: 1}, order_id: {gen: attr, name: order_id}, quantity: {gen: expr, expr: arg.line.quantity}, unit_price: {gen: expr, expr: arg.line.price}}
run: {until: 100}
"""
    )
    outcome = run_yaml_text(tmp_path, text, {})
    rows = [
        (r["task"], r["value"])
        for r in outcome.records
        if r["stream_type"] == "event"
        and isinstance(r["value"], dict)
        and "t" in r["value"]
    ]
    assert outcome.error is None, repr(outcome.error)
    orders = [v for task, v in rows if task == "items"]
    assert len(orders) == 60
    first_seen = {}
    for index, (task, value) in enumerate(rows):
        first_seen.setdefault((task, value["order_id"]), index)
    for order in orders:
        lines = [
            v
            for task, v in rows
            if task == "item" and v["order_id"] == order["order_id"]
        ]
        assert 1 <= len(lines) <= 5
        assert (
            round(sum(v["unit_price"] * v["quantity"] for v in lines), 2)
            == order["total_amount"]
        )
        assert (
            first_seen[("item", order["order_id"])]
            < first_seen[("items", order["order_id"])]
        )


def test_the_builder_makes_a_family_and_names_its_members(tmp_path):
    """Y1, Y2 and Y3 in the Builder: `add_resource(family=True, members=2)`, a task whose
    `resource_id` is `{"of", "key"}`, a signal family named by a task's `signal`, and a
    `wait` with `each`."""
    from desflow import DynamicRealtimeEnvironment  # noqa: F401
    from tests.conformance.harness import RecordingEgress

    app = SimulationContext("S", factor=0, random_seed=42)
    app.add_resource("desk", current_cap=1, family=True, members=2)
    app.add_signal("done", family=True)
    app.add_arrival_replay("a", [{"time": 0, "i": i} for i in range(4)], spawn="job")

    @app.task(
        service_id=None,
        resource_id={"of": "desk", "key": "attr.i % 2"},
        service_time="registry['S.resources.desk[0].current_cap'] + 1",
        signal={"of": "done", "key": "attr.i"},
        name="job",
        next="fan",
    )
    def job(task_id, entity):
        return {"e": "job", "i": entity.attributes["i"]}

    @app.task(
        wait={"each": "[1, 2]", "group": "all", "branch": {"timeout": "each.value"}},
        name="fan",
    )
    def fan(task_id):
        return {"e": "fan"}

    rec = RecordingEgress()
    app.add_egress(rec)
    app.run(until=50)
    jobs = [
        (round(r["sim_ts"], 3), r["value"]["i"])
        for r in rec.records
        if r["stream_type"] == "event" and r["task"] == "job" and "e" in r["value"]
    ]
    assert jobs == [(2.0, 0), (2.0, 1), (4.0, 2), (4.0, 3)]
    fans = [
        round(r["sim_ts"], 3)
        for r in rec.records
        if r["stream_type"] == "event" and r["task"] == "fan" and "e" in r["value"]
    ]
    assert fans == [4.0, 4.0, 6.0, 6.0]


def test_an_at_least_above_the_events_of_an_each_wait_fails_the_run(tmp_path):
    """Y3: `at_least` is read when the wait starts, and a number above the events the wait
    makes is a run failure that names it, because no group could be done."""
    _rows(tmp_path, 1)
    text = (
        HEAD
        + """
arrivals: {a: {replay: {path: a.csv}, spawn: job}}
tasks:
  job:
    wait: {each: 2, group: {at_least: "attr.i + 3"}, branch: {timeout: 1}}
    payload: {e: done}
run: {until: 20}
"""
    )
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is not None
    assert "which is above the 2 events it waits for" in str(outcome.error)


def test_an_index_outside_the_elements_of_an_opened_each_is_a_run_failure(tmp_path):
    """Y4: `{handle, index}` outside the elements is an `IndexError`, as for a list, and
    the message names the task, the key and the number of elements."""
    _rows(tmp_path, 1)
    text = (
        HEAD
        + """
resources: {spare: {current_cap: 1}}
arrivals: {a: {replay: {path: a.csv}, spawn: job}}
tasks:
  job:
    open: {pool: {each: 2, branch: {request: spare}}}
    payload: {e: opened}
    next: stop
  stop:
    cancel: {handle: pool, index: "attr.i + 5"}
    payload: {e: done}
run: {until: 20}
"""
    )
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is not None
    assert "index 5 is outside the 2 elements of 'pool'" in str(outcome.error)


def test_interrupt_names_one_element_of_an_opened_each_of_child_flows(tmp_path):
    """Y4: `interrupt: {handle, index}` interrupts the one child, and `interrupt` with no
    index interrupts every child in order."""
    _rows(tmp_path, 1)
    text = (
        HEAD
        + """
arrivals: {a: {replay: {path: a.csv}, spawn: job}}
tasks:
  job:
    open: {pool: {each: 3, branch: {task: child, args: {i: {gen: expr, expr: each.index}}}}}
    payload: {e: opened}
    next: pause
  pause:
    service_time: 1
    interrupt: {handle: pool, index: 1, cause: stop}
    payload: {e: paused}
  child:
    service_time: 5
    on_interrupt: abandon
    on_interrupt_next: stopped
    payload: {e: done, i: {gen: expr, expr: arg.i}}
  stopped: {payload: {e: stopped, i: {gen: expr, expr: arg.i}}}
run: {until: 50}
"""
    )
    outcome = run_yaml_text(tmp_path, text, {})
    found = [
        (t, v["e"], v.get("i")) for t, _k, v in _done(outcome) if v["e"] != "opened"
    ]
    assert (1.0, "stopped", 1) in found
    assert sorted(i for _t, e, i in found if e == "done") == [0, 2]


def test_a_renege_cancels_the_queued_requests_of_an_each_wait(tmp_path):
    """Fixes 2 and 10: a task that gives up while its `each` requests are queued leaves
    none of them in a queue, so a unit freed later is not granted to a task that has
    gone."""
    _rows(tmp_path, 1, at=1)
    text = (
        HEAD
        + """
resources: {r: {family: true, members: 2, current_cap: 1}}
arrivals:
  h: {initial: 2, spawn: hold, attributes: {i: {gen: sequence, start: 0}}}
  a: {replay: {path: a.csv}, spawn: job}
tasks:
  hold: {resource: {of: r, key: attr.i}, service_time: 10, payload: {e: held}}
  job:
    wait: {each: 2, group: all, branch: {request: {of: r, key: each.index}}}
    patience: 2
    on_renege: gone
    payload: {e: got}
  gone: {payload: {e: gone, x: {gen: expr, expr: "[queue.r[0], queue.r[1]]"}}}
run: {until: 30}
"""
    )
    outcome = run_yaml_text(tmp_path, text, {})
    gone = [(t, v["x"]) for t, _k, v in _done(outcome) if v["e"] == "gone"]
    assert gone == [(3.0, [0, 0])]
    assert not [1 for _t, _k, v in _done(outcome) if v["e"] == "got"]


def test_a_restart_keeps_the_unit_an_each_request_was_granted(tmp_path):
    """Fix 8 for `each`: an interrupt under `restart` keeps a granted request of the
    wait, as it does for named branches, so the wait made again is granted when the
    other unit is free and no unit is left held or queued."""
    outcome = run_yaml_text(
        tmp_path,
        HEAD
        + """
resources: {a: {current_cap: 1}, b: {current_cap: 1}}
arrivals: {h: {initial: 1, spawn: hold}, j: {initial: 1, spawn: job}, i: {initial: 1, spawn: cut}}
tasks:
  hold: {resource: b, service_time: 5, payload: {e: held}}
  job:
    wait: {each: [a, b], group: all, branch: {request: {of: [a, b], key: "['a', 'b'][each.index]"}}}
    on_interrupt: restart
    payload: {e: got}
    next: after
  after: {payload: {e: after, x: {gen: expr, expr: "[users.a, queue.a]"}}}
  cut: {wait: {timeout: 2}, payload: null, interrupt: {task: job, cause: x}}
run: {until: 40}
""",
        {},
    )
    found = [(t, v["e"]) for t, _k, v in _done(outcome) if v["e"] in ("got", "after")]
    assert found == [(5.0, "got"), (5.0, "after")]
    assert [v["x"] for _t, _k, v in _done(outcome) if v["e"] == "after"] == [[0, 0]]


_RESTART_EACH = """
resources: {a: {current_cap: 1}, b: {current_cap: 1}}
arrivals: {h: {initial: 1, spawn: hold}, j: {initial: 1, spawn: job}, i: {initial: 1, spawn: cut}, l: {dist: constant, value: 20, first_at: 20, max_events: 1, spawn: late}}
tasks:
  hold: {resource: b, service_time: 5, payload: {e: held}}
  job:
    wait: {each: "EACH", group: all, branch: {request: {of: [a, b], key: "each.value"}}}
    on_interrupt: restart
    payload: {e: got}
    next: after
  after: {payload: {e: after}}
  cut: {wait: {timeout: 2}, payload: null, interrupt: {task: job, cause: x}}
  late: {payload: {e: late, x: {gen: expr, expr: "[users.a, queue.a, users.b, queue.b]"}}}
run: {until: 40}
"""


@pytest.mark.parametrize(
    "elements",
    ["['b', 'a'] if t < 1 else ['b']", "['a', 'b'] if t < 1 else ['b', 'a']"],
    ids=["an element dropped", "the order reversed"],
)
def test_a_restart_that_reads_each_again_gives_back_a_kept_unit_its_leaf_does_not_ask_for(
    tmp_path, elements
):
    """A kept grant is reused only by a leaf that asks for the same resource. One for
    an element that is gone, or that now sits at another leaf, is given back and the
    leaf requests again, so no unit stays held and the wait is not blocked."""
    outcome = run_yaml_text(
        tmp_path, HEAD + _RESTART_EACH.replace("EACH", elements), {}
    )
    done = [(t, v["e"]) for t, _k, v in _done(outcome) if v["e"] in ("got", "late")]
    assert done[0] == (5.0, "got")
    late = [v["x"] for _t, _k, v in _done(outcome) if v["e"] == "late"]
    assert late == [[0, 0, 0, 0]]


def test_a_resume_keeps_the_place_of_a_request_leaf_with_a_reference(tmp_path):
    """A resume keeps every leaf's event and reads no reference again: a request made
    for the `lowest` member keeps its place at the head of that member's line, ahead of
    a task that asked for the member later, where a second pick would queue it again."""
    outcome = run_yaml_text(
        tmp_path,
        HEAD
        + """
resources: {desk: {family: true, members: 2, current_cap: 1}}
arrivals:
  h: {initial: 2, spawn: hold, attributes: {i: {gen: sequence, start: 0}}}
  j: {dist: constant, value: 1, first_at: 1, max_events: 1, spawn: job}
  i: {dist: constant, value: 1, first_at: 2, max_events: 1, spawn: cut}
  k: {dist: constant, value: 1, first_at: 1.5, max_events: 1, spawn: behind}
tasks:
  hold: {resource: {of: desk, key: attr.i}, service_time: {gen: expr, expr: "5 if attr.i == 0 else 8"}, payload: {e: held}}
  job:
    wait: {request: {of: desk, lowest: member.queue}}
    on_interrupt: resume
    service_time: 1
    payload: {e: got}
  behind: {resource: {of: desk, key: "0"}, service_time: 1, payload: {e: behind}}
  cut: {wait: {timeout: 0}, payload: null, interrupt: {task: job, cause: x}}
run: {until: 40}
""",
        {},
    )
    assert [
        (t, v["e"]) for t, _k, v in _done(outcome) if v["e"] in ("got", "behind")
    ] == [
        (6.0, "got"),
        (7.0, "behind"),
    ]


@pytest.mark.parametrize("policy", ["resume", "restart"])
def test_a_handle_leaf_with_an_index_waits_for_the_same_element_after_an_interrupt(
    tmp_path, policy
):
    """Y4: an interrupted `wait: {handle: h, index: ...}` waits for the same event
    after the interrupt under `resume` and `restart`, as the index is read once."""
    outcome = run_yaml_text(
        tmp_path,
        HEAD
        + f"""
arrivals:
  j: {{initial: 1, spawn: job}}
  i: {{dist: constant, value: 1, first_at: 2, max_events: 1, spawn: cut}}
tasks:
  job:
    open: {{pool: {{each: [5, 9], branch: {{timeout: each.value}}}}}}
    wait: {{handle: pool, index: "0 if t < 1.5 else 1"}}
    on_interrupt: {policy}
    payload: {{e: got}}
  cut: {{wait: {{timeout: 0}}, payload: null, interrupt: {{task: job, cause: x}}}}
run: {{until: 40}}
""",
        {},
    )
    assert [(t, v["e"]) for t, _k, v in _done(outcome) if v["e"] == "got"] == [
        (5.0, "got")
    ]


def test_a_task_that_balks_on_a_full_member_publishes_no_queued(tmp_path):
    """The early balk of 0.23.0 for a reference: a task whose first resource is a member
    with `queue_limit: 0` balks before it queues, as for a plain resource, and the key is
    read once."""
    _rows(tmp_path, 2)
    text = (
        HEAD
        + """
resources: {d: {family: true, members: 1, current_cap: 1, queue_limit: 0}}
arrivals: {a: {replay: {path: a.csv}, spawn: job}}
tasks:
  job: {resource: {of: d, key: "0"}, service_time: 1, payload: {e: x}}
run: {until: 30}
"""
    )
    outcome = run_yaml_text(tmp_path, text, {})
    statuses = [
        (r["key"], r["value"]["status"])
        for r in outcome.records
        if r["stream_type"] == "event" and r["task"] == "job" and "status" in r["value"]
    ]
    assert ("a-2", "queued") not in statuses
    assert ("a-2", "balked") in statuses


def test_a_queue_rank_reads_the_registry_path_of_a_member(tmp_path):
    """Y1: a queue rank that reads `registry[...]` of a member of a family loads, as the
    other places that read the registry do."""
    _rows(tmp_path, 1)
    text = (
        HEAD
        + """
resources:
  desk: {family: true, members: 1, current_cap: 1}
  r: {current_cap: 1, queue: {rank: "registry['S.resources.desk[0].current_cap']"}}
arrivals: {a: {replay: {path: a.csv}, spawn: job}}
tasks:
  job: {resource: r, service_time: 1, payload: {e: done}}
run: {until: 20}
"""
    )
    outcome = run_yaml_text(tmp_path, text, {})
    assert [(t, k) for t, k, _v in _done(outcome)] == [(1.0, "a-1")]
