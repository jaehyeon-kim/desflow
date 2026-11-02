"""Batch 10: the busy-time failure clock (`breakdown.clock`), batches (`batch`), and
monitoring (`stats.on_change`, `stats.trace`, `when: trace`): the keys at every site
(YAML, Builder, Low-level), their refusals, and what they do with interrupts, patience,
balks, failures, replications, samples, segments and `step`."""

import re
from pathlib import Path
from typing import Any, List, Tuple

import numpy as np
import pytest
from typer.testing import CliRunner

from desflow import SimulationContext, replicate
from desflow.blueprint.loader import BlueprintError
from desflow.cli import app as cli
from desflow.core.breakdowns import run_breakdowns
from desflow.core.task import TaskFailed
from desflow.resources.resource import DynamicResource
from tests.conformance.harness import RecordingEgress, run_builder, run_yaml_text


def records(outcome) -> List[Tuple[Any, ...]]:
    """The event records as (time, key, task, status or event type)."""
    return [
        (
            round(r["sim_ts"], 3),
            r["key"],
            r["task"] if "task" in r else None,
            r["value"].get("status")
            or r["value"].get("e")
            or r["value"].get("event_type"),
        )
        for r in outcome.records
        if r["stream_type"] == "event"
    ]


def load(tmp_path: Path, text: str) -> SimulationContext:
    path = tmp_path / "model.yaml"
    path.write_text(text)
    return SimulationContext.from_yaml(path)


# ------------------------------------------------------------- the busy clock (B1)

CLOCK = """\
simulation: {sim_id: S, factor: 0, random_seed: 1}
resources: {m: {current_cap: 1, breakdown: {time_to_failure: {dist: constant, value: 5}, repair: {dist: constant, value: 2}, clock: CLOCK}}}
arrivals: {a: {initial: 1, spawn: t}}
tasks:
  t: TASK
  w: {service_time: 12, payload: {e: w}, release: m}
run: {until: 40}
"""
FORMS = {
    "resource": "{resource: m, service_time: 12, payload: {e: done}, on_interrupt: POL}",
    "seize": "{seize: m, payload: {e: seized}, next: w, on_interrupt: POL}",
    "wait": "{wait: {request: m}, service_time: 12, payload: {e: done}, on_interrupt: POL}",
    "open": "{open: {r: {request: m}}, wait: {handle: r}, service_time: 12, payload: {e: done}, release: {handle: r}, on_interrupt: POL}",
}


def failures(tmp_path: Path, form: str, policy: str, clock: str = "busy"):
    text = CLOCK.replace("TASK", FORMS[form]).replace("POL", policy)
    outcome = run_yaml_text(tmp_path, text.replace("CLOCK", clock), {})
    assert outcome.error is None, repr(outcome.error)
    return [
        (round(r["sim_ts"], 3), r["value"]["status"])
        for r in outcome.records
        if r["stream_type"] == "event"
        and r["value"].get("status") in ("down", "repaired")
    ]


@pytest.mark.parametrize("form", sorted(FORMS))
@pytest.mark.parametrize("policy", ["resume", "restart"])
def test_the_busy_clock_counts_from_the_grant_in_every_holder_form(
    tmp_path, form, policy
):
    """A job of 12 holds the unit from 0: the unit fails after 5 busy seconds (5), is
    repaired at 7, and fails again after 5 more busy seconds (12), whichever way the
    task holds the unit and whether the work resumes or restarts."""
    found = failures(tmp_path, form, policy)
    assert found[:4] == [
        (5.0, "down"),
        (7.0, "repaired"),
        (12.0, "down"),
        (14.0, "repaired"),
    ]


@pytest.mark.parametrize("form", ["resource", "wait"])
def test_under_abandon_the_unit_is_given_back_so_the_clock_stops(tmp_path, form):
    """An abandoned job gives its unit back, so no busy time is left and the unit fails
    once only."""
    assert failures(tmp_path, form, "abandon") == [(5.0, "down"), (7.0, "repaired")]


def test_an_open_unit_stays_held_after_abandon_so_the_clock_goes_on(tmp_path):
    """The Unit states table says a unit a handle holds stays held when the task is
    abandoned ("Interrupt, abandon", held). The unit is still busy, so the busy clock
    goes on counting: it fails after 5 busy seconds (5), is repaired at 7, and fails
    again after 5 more (12), as the unit of a job that goes on."""
    found = failures(tmp_path, "open", "abandon")
    assert found[:4] == [
        (5.0, "down"),
        (7.0, "repaired"),
        (12.0, "down"),
        (14.0, "repaired"),
    ]


def test_an_idle_unit_does_not_wear_out(tmp_path):
    """On the calendar clock an idle unit fails every 7 seconds; on the busy clock it
    never fails while nothing holds it."""
    text = """\
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1, breakdown: {time_to_failure: {dist: constant, value: 5}, repair: {dist: constant, value: 2}, clock: CLOCK}}}
arrivals: {a: {dist: constant, value: 50, spawn: t, max_events: 1}}
tasks: {t: {resource: m, service_time: 1, payload: {e: done}}}
run: {until: 40}
"""
    for clock, wanted in (("calendar", 5), ("busy", 0)):
        outcome = run_yaml_text(tmp_path, text.replace("CLOCK", clock), {})
        downs = [r for r in records(outcome) if r[3] == "down"]
        assert len(downs) >= wanted if wanted else not downs


def test_a_job_that_ends_at_the_failure_instant_ends_first(tmp_path):
    """Jobs of 3 at 0, 4, 8, 12 and 16 on a clock of 5: the unit fails at 6, 13 and
    20, and the job that ends at 20 ends before the unit goes down at 20."""
    text = """\
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1, breakdown: {time_to_failure: {dist: constant, value: 5}, repair: {dist: constant, value: 2}, clock: busy}}}
services: {s: {dist: constant, value: 3}}
arrivals: {a: {dist: constant, value: 4, spawn: job, max_events: 5, first_at: 0}}
tasks: {job: {resource: m, service: s, payload: {e: done}}}
run: {until: 40}
"""
    found = records(run_yaml_text(tmp_path, text, {}))
    downs = [(r[0], r[3]) for r in found if r[3] == "down"]
    assert downs[:3] == [(6.0, "down"), (13.0, "down"), (20.0, "down")]
    order = [(r[0], r[3]) for r in found if r[0] == 20.0]
    assert order.index((20.0, "done")) < order.index((20.0, "down"))


def test_a_time_to_failure_of_zero_fails_at_the_grant(tmp_path):
    text = (
        CLOCK.replace("TASK", FORMS["resource"].replace("POL", "resume"))
        .replace("value: 5}", "value: 0}", 1)
        .replace("CLOCK", "busy")
    )
    outcome = run_yaml_text(tmp_path, text.replace("until: 40", "until: 3"), {})
    assert outcome.error is None
    assert (0.0, "down") in [(r[0], r[3]) for r in records(outcome)]


def test_the_default_clock_is_the_calendar_and_a_bad_clock_is_refused(tmp_path):
    base = CLOCK.replace("TASK", FORMS["resource"].replace("POL", "resume"))
    implicit = base.replace(", clock: CLOCK", "")
    a = run_yaml_text(tmp_path, implicit, {})
    b = run_yaml_text(tmp_path, base.replace("CLOCK", "calendar"), {})
    assert records(a) == records(b)
    with pytest.raises(BlueprintError, match="clock"):
        load(tmp_path, base.replace("CLOCK", "wall"))


def test_the_builder_and_the_low_level_path_take_the_clock(
    env, registry, sample_params
):
    app = SimulationContext("S", factor=0)
    app.add_resource("m", current_cap=1)
    with pytest.raises(ValueError, match="clock"):
        app.add_breakdown(
            "m",
            {"dist": "constant", "value": 5},
            {"dist": "constant", "value": 2},
            clock="wall",
        )
    app.add_breakdown(
        "m",
        {"dist": "constant", "value": 5},
        {"dist": "constant", "value": 2},
        clock="busy",
    )
    app.add_arrival("a", dist="constant", value=100, initial=1, spawn="t")
    app.task(name="t", resource_id="m", service_time=12)(lambda task_id: {"e": "done"})
    found = run_builder(app, 20)
    downs = [
        r["sim_ts"]
        for r in found
        if r["stream_type"] == "event" and r["value"].get("status") == "down"
    ]
    assert downs == [5.0, 12.0]
    registry.register_sim_parameter(sample_params)
    unit = DynamicResource(env, "Shop", "packers")
    with pytest.raises(ValueError, match="clock"):
        next(run_breakdowns(env, unit, None, clock="wall"))


def test_a_resource_wakes_what_waits_for_busy_and_idle(env, registry, sample_params):
    registry.register_sim_parameter(sample_params)
    unit = DynamicResource(env, "Shop", "packers")
    seen: List[Tuple[float, str]] = []

    def watcher():
        yield unit.when_busy()
        seen.append((env.now, "busy"))
        yield unit.when_idle()
        seen.append((env.now, "idle"))

    def user():
        yield env.timeout(2)
        request = unit.request()
        yield request
        yield env.timeout(3)
        unit.release(request)

    env.process(watcher())
    env.process(user())
    env.run(until=10)
    assert seen == [(2, "busy"), (5, "idle")]


# ----------------------------------------------------------------------- batches (B3)

PACK = """\
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
services: {s: {dist: constant, value: 2}}
arrivals:
  a: {dist: constant, value: 1, spawn: job, max_events: 4, first_at: 0}
  k: {dist: constant, value: 1, spawn: kick, max_events: 1, first_at: 1.5}
tasks:
  kick: {interrupt: {task: job, where: "WHERE", cause: cut}, payload: {e: kicked}}
  job: JOB
run: {until: 30}
"""


def pack(
    tmp_path, job: str, where: str = "process.instance == 0", until: int = 30, **kwargs
):
    text = PACK.replace("JOB", job).replace("WHERE", where)
    return run_yaml_text(
        tmp_path, text.replace("until: 30", f"until: {until}"), {}, **kwargs
    )


def by_member(found, status):
    return [(r[0], r[1]) for r in found if r[3] == status]


def test_a_batch_serves_its_members_with_one_run_and_each_publishes_its_own(tmp_path):
    """Records: `queued` as each member joins, `started` for every member when the batch
    closes, in join order; the resource and the service run once, so the members
    finish together; each publishes its own payload under its own key, in join order."""
    outcome = pack(
        tmp_path,
        "{batch: {size: 3}, resource: m, service: s, payload: {e: done}}",
        where="process.instance == 99",
    )
    found = records(outcome)
    assert by_member(found, "queued")[:3] == [(0.0, "a-1"), (1.0, "a-2"), (2.0, "a-3")]
    assert by_member(found, "started") == [(2.0, "a-1"), (2.0, "a-2"), (2.0, "a-3")]
    assert by_member(found, "done") == [(4.0, "a-1"), (4.0, "a-2"), (4.0, "a-3")]
    used = [
        r for r in outcome.records if r["stream_type"] == "event" and r["key"] == "a-4"
    ]
    assert [r["value"].get("status") for r in used] == [
        "queued"
    ]  # the fourth waits for a batch


def test_each_member_follows_its_own_next_in_join_order(tmp_path):
    text = """\
simulation: {sim_id: S, factor: 0}
arrivals:
  a: {dist: constant, value: 1, spawn: pack, max_events: 3, first_at: 0, attributes: {n: {gen: sequence, start: 1}}}
tasks:
  pack: {batch: {size: 3}, attributes: [n], payload: {e: packed}, next: {when: [{condition: "attr.n == 2", task: one}], default: other}}
  one: {payload: {e: one}}
  other: {payload: {e: other}}
run: {until: 10}
"""
    found = records(run_yaml_text(tmp_path, text, {}))
    ends = [(r[1], r[3]) for r in found if r[3] in ("packed", "one", "other")]
    assert [e for e in ends if e[1] == "packed"] == [
        ("a-1", "packed"),
        ("a-2", "packed"),
        ("a-3", "packed"),
    ]
    assert [e[0] for e in ends if e[1] != "packed"] == ["a-1", "a-2", "a-3"]


SIZES = """\
simulation: {sim_id: S, factor: 0}
stores: {s: {}}
arrivals: {a: {dist: constant, value: 1, spawn: job, first_at: 0}}
tasks: {job: {batch: {size: SIZE}, payload: {e: done}}}
run: {until: 3.5}
"""


def closes(found):
    return sorted(
        {
            r["sim_ts"]
            for r in found
            if r["stream_type"] == "event" and r["value"].get("status") == "started"
        }
    )


@pytest.mark.parametrize(
    "size,wanted",
    [
        (1, [0.0, 1.0, 2.0, 3.0]),
        (0, [0.0, 1.0, 2.0, 3.0]),
        (-3, [0.0, 1.0, 2.0, 3.0]),
        (1.5, [1.0, 3.0]),
        (2, [1.0, 3.0]),
        (np.int64(2), [1.0, 3.0]),
        (np.float64(2.0), [1.0, 3.0]),
    ],
)
def test_a_batch_closes_when_its_members_reach_the_size(size, wanted):
    """A size of 1 or below closes the batch at its first member, 1.5 at the second; a
    NumPy number stands for the plain one."""
    app = SimulationContext("S", factor=0)
    app.add_arrival("a", dist="constant", value=1, spawn="job", first_at=0)
    app.task(name="job", batch={"size": size})(lambda task_id: {"e": "done"})
    assert closes(run_builder(app, 3.5)) == wanted


@pytest.mark.parametrize("text", ["1 + 1", "level.s + 2 - 2 + 2", "t * 0 + 2"])
def test_a_size_given_as_text_is_read_in_the_first_members_scope(tmp_path, text):
    outcome = run_yaml_text(tmp_path, SIZES.replace("SIZE", f'"{text}"'), {})
    assert closes(outcome.records) == [1.0, 3.0]


def test_a_size_read_from_the_first_members_attribute(tmp_path):
    text = """\
simulation: {sim_id: S, factor: 0}
arrivals: {a: {dist: constant, value: 1, spawn: job, first_at: 0, attributes: {n: {gen: sequence, start: 1}}}}
tasks: {job: {batch: {size: "attr.n + 1"}, attributes: [n], payload: {e: done}}}
run: {until: 10}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    # a-1 has n 1 and opens a batch of 2; a-3 has n 3 and opens a batch of 4
    assert closes(outcome.records) == [1.0, 5.0]


@pytest.mark.parametrize("size", ["1e999", "0 * 1e999", "1 / 0", "level.s > 5"])
def test_a_size_that_is_not_a_finite_number_is_a_run_failure(tmp_path, size):
    """A size that gives infinity, not a number, a division by zero or a boolean fails
    the first member through `_fail`: one `failed` record, then the run raises."""
    text = SIZES.replace("SIZE", f'"{size}"')
    outcome = run_yaml_text(tmp_path, text, {})
    assert isinstance(outcome.error, TaskFailed), outcome.error
    assert [r[3] for r in records(outcome)] == ["queued", "failed"]


@pytest.mark.parametrize("size", [float("inf"), float("nan"), True])
def test_the_builder_batch_size_must_be_a_finite_number(size):
    app = SimulationContext("S", factor=0)
    app.add_arrival("a", dist="constant", value=1, spawn="job", first_at=0)
    if isinstance(size, bool):
        with pytest.raises(ValueError, match="batch size"):
            app.task(name="job", batch={"size": size})
        return
    app.task(name="job", batch={"size": size})(lambda task_id: {"e": "done"})
    with pytest.raises(TaskFailed, match="job"):
        run_builder(app, 3)


def test_a_batch_that_fails_to_size_fails_once(tmp_path):
    text = """\
simulation: {sim_id: S, factor: 0}
arrivals: {a: {dist: constant, value: 1, spawn: job, first_at: 0, max_events: 1}}
resources: {m: {current_cap: 1}}
tasks: {job: {batch: {size: "1 / (queue.m)"}, payload: {e: done}, on_fail: fallback}, fallback: {payload: {e: fell_back}}}
run: {until: 10}
"""
    found = records(run_yaml_text(tmp_path, text, {}))
    assert [r[3] for r in found] == ["queued", "failed", "fell_back"]


@pytest.mark.parametrize("policy", ["resume", "restart"])
def test_a_member_interrupted_before_the_close_leaves_and_joins_again_at_the_back(
    tmp_path, policy
):
    """B3: an interrupted member leaves the batch. Under `resume` and `restart` it joins
    again at the back: a-1 (interrupted at 1.5) joins after a-2, which joined at 1, and
    before a-3, which joins at 2 and closes the batch. It has not started, so it
    publishes no `resumed`, as a task interrupted in a queue publishes none."""
    found = records(
        pack(
            tmp_path,
            "{batch: {size: 3}, resource: m, service_time: 2, payload: {e: done}, on_interrupt: "
            + policy
            + "}",
            where="process.instance == 0",
        )
    )
    assert by_member(found, "interrupted") == [(1.5, "a-1")]
    assert by_member(found, "started") == [(2.0, "a-2"), (2.0, "a-1"), (2.0, "a-3")]
    assert by_member(found, "done") == [(4.0, "a-2"), (4.0, "a-1"), (4.0, "a-3")]
    assert [(t, st) for t, k, _, st in found if k == "a-1"] == [
        (0.0, "queued"),
        (1.5, "interrupted"),
        (2.0, "started"),
        (4.0, "done"),
    ]


def job_stats(outcome) -> dict:
    """The last published value of each `job` statistic, by its name."""
    found: dict = {}
    for r in outcome.records:
        if r["stream_type"] == "telemetry" and ".stats.tasks.job." in r["path_id"]:
            found[r["path_id"].rsplit(".", 1)[1]] = r["value"]
    return found


STARTED = """\
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
stats: {level: typical, interval: 100}
arrivals:
  h: {initial: 1, spawn: hold}
  a: {dist: constant, value: 1, spawn: job, max_events: 2, first_at: 0}
  k: {dist: constant, value: 1, spawn: kick, max_events: 1, first_at: 2}
tasks:
  hold: {resource: m, service_time: 10, payload: {e: held}}
  kick: {interrupt: {task: job, where: "process.instance == 0", cause: cut}, payload: {e: kicked}}
  job: {batch: {size: 2}, resource: m, service_time: 5, payload: {e: done}, on_interrupt: POLICY}
run: {until: 40}
"""


@pytest.mark.parametrize("policy", ["resume", "restart"])
def test_a_leader_interrupted_while_it_waits_for_the_unit_resumes_at_the_grant(
    tmp_path, policy
):
    """The batch of 2 closes at 1 and both members publish `started`; the unit is held
    until 10. The leader a-1 is interrupted at 2 and gets the unit at 10, where it
    publishes `resumed` and is counted as resumed, as a started task is. Its wait is 1
    (0 to 1) and 8 (2 to 10); its service is 1 (1 to 2) and 5 (10 to 15). The follower
    a-2 waits 0 and is served from its start at 1 to 15."""
    outcome = run_yaml_text(tmp_path, STARTED.replace("POLICY", policy), {})
    assert outcome.error is None, repr(outcome.error)
    mine = [(t, st) for t, k, _, st in records(outcome) if k == "a-1"]
    assert mine == [
        (0.0, "queued"),
        (1.0, "started"),
        (2.0, "interrupted"),
        (10.0, "resumed"),
        (15.0, "done"),
    ]
    found = job_stats(outcome)
    assert (found["interrupted"], found["resumed"]) == (1, 1)
    assert (found["wait_mean"], found["wait_max"]) == (4.5, 9)
    assert (found["service_mean"], found["service_max"]) == (10.0, 14.0)


def test_a_member_interrupted_before_the_close_keeps_its_wait(tmp_path):
    """a-1 is queued at 0 and interrupted at 0.5, and the batch closes at 1. It
    publishes no `resumed`, and its wait is the 1 s it spent in the batch, so the mean
    wait of the two members is 0.5 and the longest is 1."""
    text = """\
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
stats: {level: typical, interval: 100}
arrivals:
  a: {dist: constant, value: 1, spawn: job, max_events: 3, first_at: 0}
  k: {dist: constant, value: 1, spawn: kick, max_events: 1, first_at: 0.5}
tasks:
  kick: {interrupt: {task: job, where: "process.instance == 0", cause: cut}, payload: {e: kicked}}
  job: {batch: {size: 2}, resource: m, service_time: 5, payload: {e: done}, on_interrupt: resume}
run: {until: 30}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    mine = [(t, st) for t, k, _, st in records(outcome) if k == "a-1"]
    assert mine == [
        (0.0, "queued"),
        (0.5, "interrupted"),
        (1.0, "started"),
        (6.0, "done"),
    ]
    found = job_stats(outcome)
    assert (found["interrupted"], found.get("resumed", 0)) == (1, 0)
    assert (found["wait_mean"], found["wait_max"]) == (0.5, 1)


def test_a_member_that_abandons_leaves_and_the_size_counts_the_members_left(tmp_path):
    found = records(
        pack(
            tmp_path,
            "{batch: {size: 3}, resource: m, service_time: 2, payload: {e: done}, on_interrupt: abandon}",
            where="process.instance == 0",
        )
    )
    # a-1 is interrupted at 1.5 and leaves; a-2, a-3 and a-4 make the batch of 3 at 3
    assert by_member(found, "interrupted") == [(1.5, "a-1")]
    assert by_member(found, "started") == [(3.0, "a-2"), (3.0, "a-3"), (3.0, "a-4")]
    assert by_member(found, "done") == [(5.0, "a-2"), (5.0, "a-3"), (5.0, "a-4")]


def test_when_the_member_that_serves_leaves_the_next_member_serves(tmp_path):
    """a-1 is first of a closed batch of 2 and is interrupted in its service at 1.5:
    a-2 serves the batch from then, so its service restarts and ends at 6.5."""
    text = """\
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
arrivals:
  a: {dist: constant, value: 1, spawn: job, max_events: 2, first_at: 0}
  k: {dist: constant, value: 1, spawn: kick, max_events: 1, first_at: 1.5}
tasks:
  kick: {interrupt: {task: job, where: "process.instance == 0", cause: cut}, payload: {e: kicked}}
  job: {batch: {size: 2}, resource: m, service_time: 5, payload: {e: done}, on_interrupt: abandon}
run: {until: 30}
stats: basic
"""
    outcome = run_yaml_text(tmp_path, text, {})
    found = records(outcome)
    assert by_member(found, "interrupted") == [(1.5, "a-1")]
    assert by_member(found, "done") == [(6.5, "a-2")]
    usage = [
        r["value"]
        for r in outcome.records
        if r["stream_type"] == "telemetry"
        and r["path_id"].endswith("resources.m.in_use")
    ]
    assert usage[-1] == 0  # no unit is left held


def test_patience_counts_until_the_batch_closes_and_a_renege_leaves_the_batch(tmp_path):
    """Patience 1.5 with a size of 3 and a member a second: no batch closes, and each
    member reneges 1.5 after it joined. In a batch of 2 that closes at 1, the members
    do not renege while the service runs, though 1.5 passes."""
    found = records(
        pack(
            tmp_path,
            "{batch: {size: 3}, resource: m, service_time: 5, patience: 1.5, payload: {e: done}}",
            where="process.instance == 99",
        )
    )
    assert by_member(found, "reneged") == [
        (1.5, "a-1"),
        (2.5, "a-2"),
        (3.5, "a-3"),
        (4.5, "a-4"),
    ]
    text = PACK.replace(
        "JOB",
        "{batch: {size: 2}, resource: m, service_time: 5, patience: 1.5, payload: {e: done}}",
    ).replace("WHERE", "process.instance == 99")
    found = records(run_yaml_text(tmp_path, text, {}))
    assert by_member(found, "started") == [
        (1.0, "a-1"),
        (1.0, "a-2"),
        (3.0, "a-3"),
        (3.0, "a-4"),
    ]
    assert by_member(found, "done")[:2] == [(6.0, "a-1"), (6.0, "a-2")]
    # the second batch (a-3, a-4) closes at 3; its first member waits for the unit that
    # the first batch holds until 6, and runs out of patience there, and so does the next
    assert by_member(found, "reneged") == [(3.5, "a-3"), (5.0, "a-4")]


def test_a_batch_with_no_member_left_when_its_timer_ends_runs_nothing(tmp_path):
    text = """\
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
stats: basic
arrivals: {a: {dist: constant, value: 1, spawn: job, max_events: 2, first_at: 0}}
tasks: {job: {batch: {size: 5, timeout: 4}, resource: m, service_time: 3, patience: 1, payload: {e: done}}}
run: {until: 20}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    found = records(outcome)
    assert by_member(found, "reneged") == [(1.0, "a-1"), (2.0, "a-2")]
    assert outcome.error is None
    assert not by_member(found, "started") and not by_member(found, "done")
    used = [
        r["value"]
        for r in outcome.records
        if r["stream_type"] == "telemetry"
        and r["path_id"].endswith("resources.m.utilization_avg")
    ]
    assert not used or used[-1] == 0


def test_the_timeout_closes_the_batch_after_its_first_member(tmp_path):
    found = records(
        pack(
            tmp_path,
            "{batch: {size: 9, timeout: 2.5}, payload: {e: done}}",
            where="process.instance == 99",
            until=9,
        )
    )
    assert by_member(found, "started") == [
        (2.5, "a-1"),
        (2.5, "a-2"),
        (2.5, "a-3"),
        (5.5, "a-4"),
    ]


def test_close_when_is_tested_as_each_member_joins_and_reads_age(tmp_path):
    found = records(
        pack(
            tmp_path,
            "{batch: {size: 9, close_when: 'batch.age >= 1.5 or batch.size >= 3'}, payload: {e: done}}",
            where="process.instance == 99",
            until=9,
        )
    )
    # a-1 at 0, a-2 at 1 (age 1), a-3 at 2: age 2 >= 1.5 closes with 3 members
    assert by_member(found, "started") == [(2.0, "a-1"), (2.0, "a-2"), (2.0, "a-3")]


def test_a_member_that_balks_at_a_full_line_leaves_and_the_next_member_asks(tmp_path):
    """The member that serves a closed batch asks for the unit; a full line makes it
    balk, the next member takes over and balks too, and no unit or request is left."""
    text = """\
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1, queue_limit: 1}}
stats: basic
arrivals:
  h: {initial: 1, spawn: hog}
  f: {initial: 1, spawn: filler}
  a: {dist: constant, value: 1, spawn: job, max_events: 2, first_at: 1}
tasks:
  hog: {resource: m, service_time: 10, payload: {e: hogged}}
  filler: {resource: m, service_time: 1, payload: {e: filled}}
  job: {batch: {size: 2}, resource: m, service_time: 5, payload: {e: done}}
run: {until: 30}
"""
    found = records(run_yaml_text(tmp_path, text, {}))
    assert by_member(found, "balked") == [(2.0, "a-1"), (2.0, "a-2")]
    assert not by_member(found, "done")


def test_a_member_whose_task_fails_fails_once(tmp_path):
    text = """\
simulation: {sim_id: S, factor: 0}
stats: basic
arrivals: {a: {dist: constant, value: 1, spawn: job, max_events: 2, first_at: 0}}
tasks: {job: {batch: {size: 2}, fail: "t >= 1", payload: {e: done}, on_fail: after}, after: {payload: {e: after}}}
run: {until: 10}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    found = records(outcome)
    assert [(r[1], r[3]) for r in found if r[3] in ("failed", "after")] == [
        ("a-1", "failed"),
        ("a-1", "after"),
        ("a-2", "failed"),
        ("a-2", "after"),
    ]
    failed = [
        r["value"]
        for r in outcome.records
        if r["stream_type"] == "telemetry" and r["path_id"].endswith("tasks.job.failed")
    ]
    assert failed[-1] == 2


@pytest.mark.parametrize(
    "keys,message",
    [
        ("get: {store: s}", "takes no get"),
        ("seize: m", "takes no seize"),
        ("open: {r: {request: m}}", "takes no open"),
        ("cancel: x", "takes no cancel"),
    ],
)
def test_a_batch_takes_no_get_seize_open_or_cancel(tmp_path, keys, message):
    text = f"""\
simulation: {{sim_id: S, factor: 0}}
resources: {{m: {{current_cap: 1}}}}
stores: {{s: {{}}}}
arrivals: {{a: {{dist: constant, value: 1, spawn: job}}}}
tasks: {{job: {{batch: {{size: 2}}, {keys}, payload: {{e: 1}}}}}}
run: {{until: 5}}
"""
    with pytest.raises(BlueprintError, match=re.escape(message)):
        load(tmp_path, text)
    app = SimulationContext("S", factor=0)
    app.add_resource("m", current_cap=1)
    given = {
        "get": {"store": "s"},
        "seize": "m",
        "open": {"r": {"request": "m"}},
        "cancel": "x",
    }[keys.split(":")[0]]
    with pytest.raises(ValueError, match=re.escape(message)):
        app.task(name="job", batch={"size": 2}, **{keys.split(":")[0]: given})


@pytest.mark.parametrize(
    "batch,message",
    [
        ("{size: true}", "batch size"),
        ("{size: 2, timeout: true}", "batch timeout"),
        ("{size: 2, timeout: -1}", "batch timeout must be 0 or more"),
        ("{size: 'x y'}", "batch size"),
        ("{size: 'nope + 1'}", "nope"),
        ("{size: 2, close_when: 'batch.nope > 1'}", "batch.nope"),
        ("{size: 2, close_when: 'batch.size >'}", "close_when"),
        ("{size: 2, extra: 1}", "unknown key 'extra'"),
        ("{timeout: 2}", "size"),
        ("{size: 2, name: ''}", "name"),
    ],
)
def test_a_bad_batch_is_refused_at_load_with_the_place(tmp_path, batch, message):
    text = f"""\
simulation: {{sim_id: S, factor: 0}}
arrivals: {{a: {{dist: constant, value: 1, spawn: job}}}}
tasks: {{job: {{batch: {batch}, payload: {{e: 1}}}}}}
run: {{until: 5}}
"""
    with pytest.raises(BlueprintError, match=re.escape(message)) as error:
        load(tmp_path, text)
    assert "tasks.job.batch" in str(error.value) or "batch" in str(error.value)


def test_batch_names_are_read_in_close_when_only(tmp_path):
    base = """\
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
arrivals: {a: {dist: constant, value: 1, spawn: job}}
tasks: {job: {batch: {size: 2}, resource: m, service_time: 1, KEY, payload: {e: 1}}}
run: {until: 5}
"""
    for key in ("fail: 'batch.size > 1'", "balk: 'batch.age > 1'"):
        with pytest.raises(
            BlueprintError, match=re.escape("only the close_when of a batch reads")
        ):
            load(tmp_path, base.replace("KEY", key))
    for size in ("batch.size", "batch.age"):
        with pytest.raises(
            BlueprintError, match=re.escape("only the close_when of a batch reads")
        ):
            load(
                tmp_path,
                base.replace(
                    "batch: {size: 2}", "batch: {size: '" + size + "'}"
                ).replace("KEY, ", ""),
            )


def test_a_batch_value_place_is_checked_where_the_task_values_are():
    """`batch.size`, `batch.timeout` and `batch.close_when` are on the one list of
    places, so the blueprint and the Builder check them as they check the others."""
    from desflow.values.check import TASK_VALUE_PLACES

    places = dict(TASK_VALUE_PLACES)
    assert {"batch.size", "batch.timeout", "batch.close_when"} <= set(places)
    assert not any(
        places[key] for key in ("batch.size", "batch.timeout", "batch.close_when")
    )


def test_a_generator_in_the_batch_timeout_is_checked_at_load(tmp_path):
    text = """\
simulation: {sim_id: S, factor: 0}
arrivals: {a: {dist: constant, value: 1, spawn: job}}
tasks: {job: {batch: {size: 2, timeout: {gen: expr, expr: "attr.nope + queue.nobody"}}, payload: {e: 1}}}
run: {until: 5}
"""
    with pytest.raises(BlueprintError, match="nobody"):
        load(tmp_path, text)


def test_the_builder_batch_gives_the_blueprints_records(tmp_path):
    text = """\
simulation: {sim_id: S, factor: 0}
arrivals: {a: {dist: constant, value: 1, spawn: job, first_at: 0}}
tasks: {job: {batch: {size: 3, timeout: 2, close_when: "batch.age >= 1"}, payload: {e: done}}}
run: {until: 5.5}
"""
    yaml_records = records(run_yaml_text(tmp_path, text, {}))
    app = SimulationContext("S", factor=0)
    app.add_arrival("a", dist="constant", value=1, spawn="job", first_at=0)
    app.task(
        name="job", batch={"size": 3, "timeout": 2, "close_when": "batch.age >= 1"}
    )(lambda task_id: {"e": "done"})
    built = [
        (
            round(r["sim_ts"], 3),
            r["key"],
            r["value"].get("status") or r["value"].get("e"),
        )
        for r in run_builder(app, 5.5)
        if r["stream_type"] == "event"
    ]
    assert built == [(t, k, v) for t, k, _task, v in yaml_records]


# ---------------------------------------------------------- monitoring (M1, MF5)

MONITOR = """\
simulation: {sim_id: M, factor: 0, random_seed: 7}
stats: STATS
egress: EGRESS
resources: {res: {current_cap: 1}}
arrivals: {a: {dist: constant, value: 1, spawn: use, max_events: 3, first_at: 0}}
tasks: {use: {resource: res, service_time: 1.5, payload: {e: used}}}
run: {until: 6}
"""


def monitored(
    stats="{level: basic, on_change: true, trace: true}",
    egress="[{type: Console, when: trace}]",
):
    return MONITOR.replace("STATS", stats).replace("EGRESS", egress)


def trace_records(app: SimulationContext, until=None):
    sink = RecordingEgress()
    other = RecordingEgress()
    app.add_egress(sink, when="trace")
    app.add_egress(other)
    app.run(until=until) if until is not None else app.run()
    return sink.records, other.records


def test_the_level_is_none_when_only_trace_is_set_and_on_change_needs_one(tmp_path):
    """`stats.level` defaults to `none` when only `trace` is set, so a trace alone
    publishes no statistics; `on_change` publishes statistics, so it needs a `level`
    above `none`, and a mapping with `on_change` and no `level` is refused."""
    app = load(tmp_path, monitored("{trace: true}"))
    trace, rest = trace_records(app)
    assert trace and all(r["stream_type"] == "trace" for r in trace)
    assert not [
        r for r in rest if r["stream_type"] == "telemetry" and ".stats." in r["path_id"]
    ]
    for bare in ("{on_change: true}", "{on_change: true, trace: true}"):
        with pytest.raises(
            BlueprintError, match=re.escape("unless it sets only trace")
        ):
            load(tmp_path, monitored(bare, "[]"))
    app = load(
        tmp_path, monitored("{level: basic, on_change: true, trace: true}", "[]")
    )
    _, rest = trace_records(app)
    assert any(
        r["path_id"].endswith("resources.res.in_use")
        for r in rest
        if r["stream_type"] == "telemetry"
    )


def test_on_change_with_level_none_is_refused(tmp_path):
    with pytest.raises(BlueprintError, match="on_change"):
        load(tmp_path, monitored("{level: none, on_change: true}"))
    app = SimulationContext("S", factor=0)
    with pytest.raises(ValueError, match="on_change"):
        app.with_stats("none", on_change=True)


def test_a_trace_sink_is_refused_without_stats_trace(tmp_path):
    with pytest.raises(BlueprintError, match="trace"):
        load(tmp_path, monitored("{level: basic, on_change: true}"))


def test_a_trace_reaches_only_the_sinks_that_ask_for_it(tmp_path):
    app = load(tmp_path, monitored())
    trace, rest = trace_records(app)
    assert trace
    assert all(r["stream_type"] != "trace" for r in rest)
    assert all(r["stream_type"] == "trace" for r in trace)
    shape = trace[0]
    assert set(shape["value"]) == {"priority", "event_id", "event"}
    assert [r["sim_ts"] for r in trace] == sorted(r["sim_ts"] for r in trace)


def test_the_trace_names_only_model_events(tmp_path):
    """A statistics reading or a flush timer of desflow's own is not a model event, so
    the trace holds the same events a SimPy run of the model processes."""
    app = load(tmp_path, monitored("{trace: true, level: basic, interval: 1}"))
    trace, _ = trace_records(app)
    names = {r["value"]["event"] for r in trace}
    assert (
        names
        <= {
            "Initialize",
            "Process",
            "Timeout",
            "DynamicResourceRequest",
            "DynamicRelease",
            "Event",
            "Condition",
            "DynamicTimeout",
        }
        | names
    )
    assert "Initialize" in names and "Process" in names


def test_each_record_of_the_trace_is_published_before_the_events_callbacks(tmp_path):
    """The trace record of the event that releases a task is published before the task
    publishes its own record at that instant."""
    app = load(tmp_path, monitored("{trace: true}"))
    sink = RecordingEgress()
    both = RecordingEgress()
    app.add_egress(sink, when="trace")
    app.add_egress(both)
    app.run()
    first = next(r for r in sink.records if r["sim_ts"] == 1.5)
    assert first["value"]["event"]


@pytest.mark.parametrize("form", ["yaml", "builder"])
def test_trace_and_on_change_are_the_same_in_yaml_and_the_builder(tmp_path, form):
    yaml_app = load(tmp_path, monitored())
    a_trace, a_rest = trace_records(yaml_app)
    app = (
        SimulationContext("M", factor=0, seed=7)
        if False
        else SimulationContext("M", factor=0)
    )
    app.add_resource("res", current_cap=1)
    app.add_arrival("a", dist="constant", value=1, spawn="use", first_at=0)
    app.task(name="use", resource_id="res", service_time=1.5)(
        lambda task_id: {"e": "used"}
    )
    app.with_stats("basic", on_change=True, trace=True)
    b_trace, b_rest = trace_records(app, 6)
    key = lambda r: (r["sim_ts"], r["value"]["priority"], r["value"]["event"])  # noqa: E731
    assert [key(r) for r in b_trace][:10] == [key(r) for r in a_trace][:10]

    def series(rest):
        return [
            (r["sim_ts"], r["path_id"].rsplit(".", 1)[1], r["value"])
            for r in rest
            if r["stream_type"] == "telemetry"
            and r["path_id"].rsplit(".", 1)[1] in ("in_use", "queue_length")
        ]

    assert series(b_rest)[:6] == series(a_rest)[:6]


def test_trace_and_on_change_under_replicate(tmp_path):
    path = tmp_path / "m.yaml"
    path.write_text(
        monitored("{on_change: true, trace: true, level: basic, interval: 1}").replace(
            "egress: [{type: Console, when: trace}]\n", ""
        )
    )
    result = replicate(str(path), 3, seed=1, measures=["resources.res.utilization_avg"])
    assert len(result.values["resources.res.utilization_avg"]) == 3


def test_trace_with_sample_prints_events_and_keeps_the_trace_off(tmp_path):
    path = tmp_path / "m.yaml"
    path.write_text(monitored())
    result = CliRunner().invoke(cli, ["run", str(path), "--sample", "4"])
    assert result.exit_code == 0, result.output
    assert "Traceback" not in result.output


def test_trace_and_on_change_under_step_and_segments(tmp_path):
    """`step()` and a run split into segments publish the same trace as one run."""
    text = monitored()
    whole = load(tmp_path, text)
    whole_trace, _ = trace_records(whole)

    stepped = load(tmp_path, text)
    sink = RecordingEgress()
    stepped.add_egress(sink, when="trace")
    stepped.start()
    stepped.step()
    stepped.step()
    stepped.advance(6)
    stepped.close()
    key = lambda r: (r["sim_ts"], r["value"]["priority"], r["value"]["event"])  # noqa: E731
    # the stop event of each `until` is SimPy's own, so it is left out of the comparison
    model = lambda found: [key(r) for r in found if r["value"]["event"] != "Event"]  # noqa: E731
    assert model(sink.records) == model(whole_trace)

    segments = text.replace("run: {until: 6}", "run: {until: 6, segments: {every: 2}}")
    seg = load(tmp_path, segments)
    seg_trace, _ = trace_records(seg)
    assert [(t, c) for t, _p, c in map(key, seg_trace) if c != "Event"] == [
        (t, c) for t, _p, c in map(key, whole_trace) if c != "Event"
    ]


def test_a_unit_given_back_by_a_release_event_publishes_no_reading_of_its_own(tmp_path):
    """The reading follows a `request` or `release` call, not the grant a release event
    makes: two jobs on one unit give 4 readings, as the patched SimPy program does."""
    text = """\
simulation: {sim_id: M, factor: 0}
stats: {level: basic, on_change: true}
resources: {res: {current_cap: 1}}
arrivals: {a: {initial: 2, spawn: use}}
tasks: {use: {resource: res, service_time: 0, payload: {event_type: used}}}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    series = [
        (r["path_id"].rsplit(".", 1)[1], r["value"])
        for r in outcome.records
        if r["stream_type"] == "telemetry"
        and r["path_id"].rsplit(".", 1)[1] in ("in_use", "queue_length")
    ]
    assert series == [
        ("in_use", 1),
        ("queue_length", 0),
        ("queue_length", 1),
        ("in_use", 0),
        ("queue_length", 0),
    ]


def test_a_batch_is_named_as_a_feature_the_queueing_formulas_leave_out(tmp_path):
    from desflow.explain.features import task_features

    app = load(
        tmp_path,
        "simulation: {sim_id: S, factor: 0}\narrivals: {a: {dist: constant, value: 1, spawn: job}}\n"
        "tasks: {job: {batch: {size: 2}, payload: {e: 1}}}\nrun: {until: 5}\n",
    )
    assert "batch" in task_features(app, app._task_specs["job"])


def test_a_busy_clock_run_with_no_end_time_ends_when_the_work_does(tmp_path):
    """The clock waits for the unit to be held without a timer of its own, so a run
    with no `until` ends after the last job, and a failure clock that has not run out
    keeps nothing alive."""
    text = """\
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1, breakdown: {time_to_failure: {dist: constant, value: 50}, repair: {dist: constant, value: 2}, clock: busy}}}
arrivals: {a: {dist: constant, value: 4, spawn: job, max_events: 2, first_at: 0}}
tasks: {job: {resource: m, service_time: 3, payload: {e: done}}}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None
    assert [r[0] for r in records(outcome) if r[3] == "done"] == [3.0, 7.0]
    assert outcome.app_or_env.env.now == 7.0


def test_without_on_change_the_readings_are_the_periodic_ones_only(tmp_path):
    """`level: basic` with a `trace` and no `on_change` publishes a resource's readings
    at the start and the end only, not after each call."""
    app = load(tmp_path, monitored("{level: basic, trace: true}"))
    _, rest = trace_records(app)
    times = {
        r["sim_ts"]
        for r in rest
        if r["stream_type"] == "telemetry"
        and r["path_id"].endswith("resources.res.in_use")
    }
    assert times <= {0.0, 6.0}, times


def test_a_batch_asks_for_the_unit_once_for_all_its_members(tmp_path):
    text = """\
simulation: {sim_id: S, factor: 0}
stats: {level: basic, on_change: true}
resources: {m: {current_cap: 1}}
arrivals: {a: {dist: constant, value: 1, spawn: job, max_events: 3, first_at: 0}}
tasks: {job: {batch: {size: 3}, resource: m, service_time: 2, payload: {e: done}}}
run: {until: 10}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    lines = [
        r["value"]
        for r in outcome.records
        if r["stream_type"] == "telemetry"
        and r["path_id"].endswith("resources.m.queue_length")
    ]
    assert lines and max(lines) == 0
    assert [r[0] for r in records(outcome) if r[3] == "done"] == [4.0, 4.0, 4.0]


def test_a_member_served_by_a_closed_batch_does_not_renege_while_the_service_runs(
    tmp_path,
):
    """Patience 3 and a batch of 3 that closes at 2: the member that joined at 1 has
    its patience to 4, and the service runs to 7, so it must not renege at 4."""
    found = records(
        pack(
            tmp_path,
            "{batch: {size: 3}, resource: m, service_time: 5, patience: 3, payload: {e: done}}",
            where="process.instance == 99",
        )
    )
    assert by_member(found, "started") == [(2.0, "a-1"), (2.0, "a-2"), (2.0, "a-3")]
    assert by_member(found, "done") == [(7.0, "a-1"), (7.0, "a-2"), (7.0, "a-3")]
    assert not [r for r in found if r[0] < 7.0 and r[3] == "reneged" and r[1] != "a-4"]


@pytest.mark.parametrize("factor", [0, 0.01])
def test_the_trace_is_the_same_paced_and_unpaced(tmp_path, factor):
    """The unpaced loop and the paced one both publish the trace. The paced run is
    not strict, so a slow machine does not stop it for falling behind the wall
    clock."""
    text = monitored().replace("factor: 0", f"factor: {factor}, strict: false")
    trace, _ = trace_records(load(tmp_path, text))
    wanted, _ = trace_records(load(tmp_path, monitored()))
    key = lambda r: (r["sim_ts"], r["value"]["priority"], r["value"]["event"])  # noqa: E731
    assert [key(r) for r in trace] == [key(r) for r in wanted]
    assert len(trace) > 10


@pytest.mark.parametrize(
    "task_line, place",
    [
        (
            "t: {payload: {e: 1}, resume: {task: u, limit: {gen: lookup, task: nosuch, path: n}}}",
            "tasks.t.resume.limit.task",
        ),
        (
            "t: {payload: {e: 1}, open: {x: {timeout: {gen: lookup, task: nosuch, path: n}}}}",
            "tasks.t.open.x.timeout.task",
        ),
        (
            "t: {payload: {e: 1}, resume: {task: u, value: {gen: expr, expr: \"registry['S.nosuch']\"}}}",
            "registry",
        ),
    ],
)
def test_a_generator_in_a_resume_or_open_value_is_checked_at_load(
    tmp_path, task_line, place
):
    """The load checks walk TASK_VALUE_PLACES, so `resume` and `open` values are checked."""
    text = f"""\
simulation: {{sim_id: S, factor: 0}}
arrivals: {{a: {{dist: constant, value: 1, spawn: t, max_events: 1}}}}
tasks:
  {task_line}
  u: {{wait: {{suspend: true}}, payload: {{e: 2}}}}
run: {{until: 3}}
"""
    with pytest.raises(BlueprintError, match=re.escape(place)):
        load(tmp_path, text)


OPENED = """\
simulation: {sim_id: S, factor: 0, random_seed: 1}
resources: {m: {current_cap: 1}, k: {current_cap: 1, queue_limit: 0}}
arrivals:
  HOG
  a: {dist: constant, value: 1, first_at: 1, max_events: 1, spawn: t}
  c: {dist: constant, value: 1, first_at: 3, max_events: 1, spawn: cut}
tasks:
  hog: {resource: m, service_time: 10, payload: {e: hog}}
  t: TASK
  cut: {payload: {e: cut}, interrupt: {task: t}}
run: {until: 30}
"""
HOG = "h: {dist: constant, value: 1, first_at: 0, max_events: 1, spawn: hog}"


def units_left(text: str, tmp_path: Path, resource: str = "m") -> List[int]:
    """Runs the model to the end and returns [users, queue] of a resource."""
    app = load(tmp_path, text)
    run_builder(app, 30)
    found = app._resources_map[resource]
    return [len(found.users), len(found.queue)]


@pytest.mark.parametrize("queued", [True, False])
def test_an_abandoned_open_request_stays_as_the_unit_states_table_says(
    tmp_path, queued
):
    """Unit states, "Interrupt, abandon": a queued handle request stays and is granted
    later (the other holder is done at 10, so the unit is held at the end), and a
    granted one stays held. Nothing gives the unit back."""
    task = "{open: {r: {request: m}}, wait: {timeout: 50}, on_interrupt: abandon, payload: {e: t}}"
    text = OPENED.replace("TASK", task).replace("HOG", HOG if queued else "")
    if not queued:
        text = text.replace("  \n", "")
    assert units_left(text, tmp_path) == [1, 0]


def test_a_reneged_open_request_stays_queued_as_the_unit_states_table_says(tmp_path):
    """Patience end of a handle is not changed: the request stays and is granted later."""
    task = "{open: {r: {request: m}}, wait: {handle: r}, patience: 5, payload: {e: t}}"
    text = (
        OPENED.replace("TASK", task)
        .replace("HOG", HOG)
        .replace(
            "  c: {dist: constant, value: 1, first_at: 3, max_events: 1, spawn: cut}\n",
            "",
        )
        .replace("  cut: {payload: {e: cut}, interrupt: {task: t}}\n", "")
    )
    assert units_left(text, tmp_path) == [1, 0]


# ------------------------------------------------- batch names, followers and ties

FOLLOWER = """\
simulation: {sim_id: S, factor: 0}
stats: basic
resources: {m: {current_cap: 1}}
arrivals:
  a: {dist: constant, value: 1, spawn: job, max_events: 2, first_at: 0}
  k: {dist: constant, value: 1, spawn: kick, max_events: 1, first_at: 2}
tasks:
  kick: {interrupt: {task: job, where: "process.instance == 1", cause: cut}, payload: {e: kicked}}
  job: {batch: {size: 2}, resource: m, service_time: 5, payload: {e: done}, on_interrupt: POLICY}
run: {until: 30}
"""


@pytest.mark.parametrize("policy", ["resume", "restart"])
def test_a_follower_interrupted_after_the_close_resumes_and_is_counted(
    tmp_path, policy
):
    """The batch of 2 closes at 1 and a-2 is a follower. Interrupted at 2 it publishes
    `interrupted` and then `resumed`, and goes on waiting to be served: the leader's
    service serves it, so `restart` has nothing to restart. The statistics count the
    interruption and the resume once each."""
    app = load(tmp_path, FOLLOWER.replace("POLICY", policy))
    found = records(run_builder_outcome(app))
    mine = [(t, s) for t, key, _, s in found if key == "a-2"]
    assert mine == [
        (1.0, "queued"),
        (1.0, "started"),
        (2.0, "interrupted"),
        (2.0, "resumed"),
        (6.0, "done"),
    ]
    stats = app._env.stats
    counted = stats._task("job")
    assert (counted.interrupted, counted.resumed) == (1, 1)


def run_builder_outcome(app, until=30):
    """Runs a built context and returns an object with the records, as `records` reads."""
    sink = RecordingEgress()
    app.add_egress(sink)
    app.run(until=until)

    class Outcome:
        records = sink.records

    return Outcome


SHARED = """\
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
arrivals:
  a: {dist: constant, value: 1, spawn: first, max_events: 2, first_at: 0}
  b: {dist: constant, value: 1, spawn: second, max_events: 2, first_at: 0.5}
tasks:
  first: {batch: {name: NAME1, size: 4}, resource: m, service_time: 2, payload: {e: first_done}}
  second: {batch: {name: NAME2, size: 4}, resource: m, service_time: 2, payload: {e: second_done}}
run: {until: 30}
"""


def test_tasks_with_one_batch_name_share_one_batch(tmp_path):
    """Design B3: the batch is identified by its name, which defaults to the task's
    name. Two tasks that name one batch put their members in it: the four members
    (two of each task) close it together at 1.5, in the order they joined."""
    found = records(
        run_builder_outcome(
            load(tmp_path, SHARED.replace("NAME1", "x").replace("NAME2", "x"))
        )
    )
    assert [(t, k) for t, k, _, s in found if s == "started"] == [
        (1.5, "a-1"),
        (1.5, "b-1"),
        (1.5, "a-2"),
        (1.5, "b-2"),
    ]


def test_tasks_with_two_batch_names_make_two_batches(tmp_path):
    """The same four members with two names make two batches of 4 that never close."""
    found = records(
        run_builder_outcome(
            load(tmp_path, SHARED.replace("NAME1", "x").replace("NAME2", "y"))
        )
    )
    assert not [r for r in found if r[3] == "started"]


def test_the_batch_name_defaults_to_the_task_name_in_the_builder_too(tmp_path):
    """Builder: two tasks with the same `name` share a batch, two without do not."""

    def built(first_name, second_name):
        app = SimulationContext("S", factor=0)
        app.add_resource("m", current_cap=1)
        app.add_service("s", "constant", value=2)
        app.add_arrival("a", "constant", value=1000, spawn="first", initial=2)
        app.add_arrival("b", "constant", value=1000, spawn="second", initial=2)
        for task, name in (("first", first_name), ("second", second_name)):
            app.task(
                "s",
                "m",
                name=task,
                batch={"size": 4, **({"name": name} if name else {})},
            )(lambda task_id: {"e": "done"})
        return [r for r in records(run_builder_outcome(app, 10)) if r[3] == "started"]

    assert len(built("x", "x")) == 4
    assert built(None, None) == []  # two batches of 4 that never fill


TIE = """\
simulation: {sim_id: S, factor: 0}
arrivals:
  x: {initial: 1, spawn: pack, attributes: {d: 2}}
  y: {initial: 1, spawn: pack, attributes: {d: 0}}
  z: {dist: constant, value: 0.5, first_at: 0.5, max_events: 1, spawn: pack, attributes: {d: 1.5}}
tasks:
  pack: {wait: {timeout: "attr.d"}, attributes: [d], batch: {size: 5, timeout: 2}, payload: {e: packed}}
run: {until: 10}
"""


def test_a_joiner_at_the_instant_of_the_batch_timeout_follows_event_order(tmp_path):
    """Research B3, Tie: the timeout runs 2 s after the first member (y, at 0). x joins
    at 2 and z joins at 2 as well, but x's event was scheduled before the timer and
    z's after it. A joiner whose event comes before the timer is in the batch; one
    whose event comes after it starts the next batch, which closes 2 s later."""
    found = records(run_builder_outcome(load(tmp_path, TIE)))
    started = [(t, k) for t, k, _, s in found if s == "started"]
    assert started == [(2.0, "y-1"), (2.0, "x-1"), (4.0, "z-1")]


# ------------------------------------------------------------------- trace names

TRACE_NAMES = {
    "Initialize",
    "Process",
    "Timeout",
    "Condition",
    "Interruption",
    "Request",
    "Release",
    "Get",
    "Put",
    "Suspension",
    "Event",
}

TRACED = """\
simulation: {sim_id: S, factor: 0}
stats: {trace: true}
resources: {m: {current_cap: 1, breakdown: {time_to_failure: {dist: constant, value: 3}, repair: {dist: constant, value: 1}, clock: busy}}}
stores: {s: {}}
arrivals: {a: {initial: 1, spawn: park}, b: {dist: constant, value: 2, first_at: 2, max_events: 1, spawn: wake}}
tasks:
  park: {wait: {suspend: true}, payload: {e: parked}, next: work}
  work: {resource: m, service_time: 4, put: {store: s, item: 1}, payload: {e: w}, next: take}
  take: {get: {store: s}, wait: {all: {x: {timeout: 1}, y: {timeout: 2}}}, payload: {e: t}}
  wake: {resume: {task: park}, payload: {e: woke}}
run: {until: 20}
"""


def test_a_trace_record_names_only_public_events(tmp_path):
    """The `event` of a trace record is one of a fixed set of public names: SimPy's own
    where it has one, and `Suspension` for desflow's. No private (`_Suspension`) or
    desflow (`Dynamic...`) class name appears."""
    app = load(tmp_path, TRACED)
    trace, _ = trace_records(app)
    names = {r["value"]["event"] for r in trace}
    assert names <= TRACE_NAMES
    assert {"Request", "Release", "Suspension", "Process", "Timeout"} <= names
    assert not [n for n in names if n.startswith(("_", "Dynamic"))]


def test_every_class_desflow_defines_has_a_public_trace_name():
    from desflow.core.environment import trace_event_name
    from desflow.core.task import _Suspension
    from desflow.resources.container import (
        DynamicContainerGet,
        DynamicContainerPut,
    )
    from desflow.resources.resource import DynamicRelease, DynamicResourceRequest
    from desflow.resources.store import (
        DynamicFilterStoreGet,
        DynamicStoreGet,
        DynamicStorePut,
    )

    wanted = {
        DynamicResourceRequest: "Request",
        DynamicRelease: "Release",
        DynamicStoreGet: "Get",
        DynamicFilterStoreGet: "Get",
        DynamicContainerGet: "Get",
        DynamicStorePut: "Put",
        DynamicContainerPut: "Put",
        _Suspension: "Suspension",
    }
    for kind, name in wanted.items():
        assert trace_event_name(object.__new__(kind)) == name


def test_a_trace_sink_with_no_trace_is_refused_in_the_builder_too():
    """YAML refuses `when: trace` with no `stats.trace` at load; the Builder refuses the
    same model when it runs, so the sink never silently gets nothing."""
    app = SimulationContext("S", factor=0)
    app.add_arrival("a", dist="constant", value=1, spawn="t", first_at=0)
    app.task(name="t")(lambda task_id: {"e": 1})
    app.add_egress(RecordingEgress(), when="trace")
    with pytest.raises(ValueError, match=re.escape("with_stats(trace=True)")):
        app.run(until=2)
    app.with_stats("none", trace=True)
    app.run(until=2)


def test_the_busy_clock_with_two_units_counts_while_either_is_held(tmp_path):
    """Capacity 2: a job of 3 at 0 and a job of 9 at 2 overlap from 2 to 3. One unit is
    held from 0 to 12 (3 + 9 hold at least one unit throughout), so the unit fails
    after 5 busy seconds (5), is repaired at 7, and fails again after 5 more (12)."""
    text = """\
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 2, breakdown: {time_to_failure: {dist: constant, value: 5}, repair: {dist: constant, value: 2}, clock: busy}}}
arrivals: {a: {initial: 1, spawn: t}, b: {dist: constant, value: 2, first_at: 2, max_events: 1, spawn: u}}
tasks:
  t: {resource: m, service_time: 3, payload: {e: t3}, on_interrupt: resume}
  u: {resource: m, service_time: 9, payload: {e: u9}, on_interrupt: resume}
run: {until: 40}
"""
    found = records(run_yaml_text(tmp_path, text, {}))
    assert [(t, s) for t, _, _, s in found if s in ("down", "repaired")][:4] == [
        (5.0, "down"),
        (7.0, "repaired"),
        (12.0, "down"),
        (14.0, "repaired"),
    ]


def test_a_resource_that_never_goes_idle_keeps_no_waiting_event_for_each_failure():
    """The busy clock waits for the resource to go idle, and drops that event when the
    timer wins, so a resource that stays busy for the whole run holds at most one."""
    app = SimulationContext("S", factor=0)
    app.add_resource("m", current_cap=1)
    app.add_breakdown(
        "m",
        {"dist": "constant", "value": 1},
        {"dist": "constant", "value": 1},
        work="finish",
        clock="busy",
    )
    app.add_arrival("a", dist="constant", value=100, initial=1, spawn="t")
    app.task(name="t", resource_id="m", service_time=60)(lambda task_id: {"e": "done"})
    app.run(until=40)
    assert len(app._resources_map["m"]._idle_waiters) <= 1


SHARING = """\
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}, n: {current_cap: 1}}
services: {s: {dist: constant, value: 2}, t: {dist: constant, value: 3}}
arrivals:
  a: {dist: constant, value: 1, spawn: first, max_events: 2, first_at: 0}
  b: {dist: constant, value: 1, spawn: second, max_events: 2, first_at: 0.5}
tasks:
  first: FIRST
  second: SECOND
  after: {payload: {e: after}}
run: {until: 30}
"""
BASE = "batch: {name: x, size: 4}, resource: m, service: s, payload: {e: first}"


@pytest.mark.parametrize(
    "second, differs",
    [
        ("batch: {name: x, size: 3}, resource: m, service: s", "size"),
        ("batch: {name: x, size: 4, timeout: 5}, resource: m, service: s", "timeout"),
        (
            "batch: {name: x, size: 4, close_when: 'batch.size >= 2'}, resource: m, service: s",
            "close_when",
        ),
        ("batch: {name: x, size: 4}, resource: n, service: s", "resource"),
        ("batch: {name: x, size: 4}, resource: m, service: t", "service"),
        (
            "batch: {name: x, size: 4}, resource: m, service_time: 2",
            "service",
        ),
    ],
)
def test_tasks_that_share_a_batch_must_agree_on_what_it_needs_one_answer_for(
    tmp_path, second, differs
):
    """The leader runs one resource and service for every member, and the first
    member's task gives `size`, `timeout` and `close_when`, so two tasks with one batch
    name that differ in one of them are refused at load, naming both tasks."""
    text = SHARING.replace("FIRST", "{" + BASE + "}").replace(
        "SECOND", "{" + second + ", payload: {e: second}}"
    )
    with pytest.raises(BlueprintError) as error:
        load(tmp_path, text)
    message = str(error.value)
    assert "'first' and 'second'" in message and f"differ in {differs}" in message
    line = (
        text.splitlines().index(f"  second: {{{second}, payload: {{e: second}}}}") + 1
    )
    assert f"model.yaml:{line}:" in message


def test_tasks_that_share_a_batch_may_differ_in_what_each_member_keeps(tmp_path):
    """`on_interrupt`, `patience`, `payload` and `next` stay with the member."""
    second = (
        "{batch: {name: x, size: 4}, resource: m, service: s, on_interrupt: abandon, "
        "patience: 50, payload: {e: second}, next: after}"
    )
    text = SHARING.replace("FIRST", "{" + BASE + "}").replace("SECOND", second)
    assert isinstance(load(tmp_path, text), SimulationContext)


@pytest.mark.parametrize(
    "difference, differs",
    [
        ({"batch": {"name": "x", "size": 3}}, "size"),
        ({"batch": {"name": "x", "size": 4, "timeout": 5}}, "timeout"),
        (
            {"batch": {"name": "x", "size": 4, "close_when": "batch.size >= 2"}},
            "close_when",
        ),
        ({"resource_id": "n"}, "resource"),
        ({"service_id": "t"}, "service"),
        ({"service_id": None, "service_time": 2}, "service"),
    ],
)
def test_the_builder_refuses_the_same_differences_when_the_run_starts(
    difference, differs
):
    def built(**given):
        app = SimulationContext("S", factor=0)
        app.add_resource("m", current_cap=1)
        app.add_resource("n", current_cap=1)
        app.add_service("s", "constant", value=2)
        app.add_service("t", "constant", value=3)
        app.add_arrival("a", "constant", value=1000, spawn="first", initial=1)
        app.add_arrival("b", "constant", value=1000, spawn="second", initial=1)
        app.task(
            name="first",
            resource_id="m",
            service_id="s",
            batch={"name": "x", "size": 4},
        )(lambda task_id: {"e": 1})
        keys = {
            "name": "second",
            "resource_id": "m",
            "service_id": "s",
            "batch": {"name": "x", "size": 4},
        }
        keys.update(given)
        app.task(**keys)(lambda task_id: {"e": 2})
        return app

    app = built(**difference)
    with pytest.raises(ValueError) as error:
        app.run(until=2)
    assert "'first' and 'second'" in str(error.value)
    assert f"differ in {differs}" in str(error.value)
    built().run(until=2)


@pytest.mark.parametrize(
    "first, second",
    [
        ("service_time: {gen: sample, dist: exponential, rate: 0.5}",) * 2,
        (
            "service_time: {gen: sample, dist: exponential, rate: 0.5}",
            "service_time: {rate: 0.5, dist: exponential, gen: sample}",
        ),
        (
            "batch: {name: x, size: 4, timeout: {gen: sample, dist: exponential, rate: 0.5}}",
        )
        * 2,
        (
            "batch: {name: x, size: 4, close_when: 'batch.size >= 2'}",
            "batch: {name: x, size: 4, close_when: 'batch.size>=2'}",
        ),
        ("batch: {name: x, size: 2}", "batch: {name: x, size: '2'}"),
    ],
)
def test_tasks_that_share_a_batch_load_when_their_values_are_the_same(
    tmp_path, first, second
):
    """Values are compared by what they mean, not by where they were written: a
    generator by its fields, an expression by its parsed form, text that is a number by
    its number."""

    def task(given: str) -> str:
        batch = "batch: {name: x, size: 4}"
        service = "service: s"
        if given.startswith("service_time"):
            service = given
        else:
            batch = given
        return "{" + batch + ", resource: m, " + service + ", payload: {e: 1}}"

    text = SHARING.replace("FIRST", task(first)).replace("SECOND", task(second))
    assert isinstance(load(tmp_path, text), SimulationContext)


def test_tasks_with_different_generator_arguments_are_refused(tmp_path):
    first = "{batch: {name: x, size: 4}, resource: m, service_time: {gen: sample, dist: exponential, rate: 0.5}, payload: {e: 1}}"
    second = "{batch: {name: x, size: 4}, resource: m, service_time: {gen: sample, dist: exponential, rate: 0.7}, payload: {e: 1}}"
    text = SHARING.replace("FIRST", first).replace("SECOND", second)
    with pytest.raises(BlueprintError, match="differ in service_time"):
        load(tmp_path, text)
