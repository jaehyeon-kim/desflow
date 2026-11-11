"""C7 patience: reneging after a wait, and balking before joining.

Reneging: a holder takes the one machine and keeps it; a waiter with a patience
leaves the queue when the patience runs out before a unit is granted. When the
grant and the patience timeout fall at the same instant, SimPy's outcome depends
on which event was scheduled first, and desflow follows the same order in all
three APIs. Balking: a task whose balk condition holds never joins the queue, so
it creates no request and is counted once.
"""

import functools

import pytest
import simpy
from simpy.resources.resource import SortedQueue

from desflow import (
    CapacityConfig,
    DynamicResource,
    Entity,
    SimParameter,
)
from desflow.builder.context import SimulationContext
from tests.conformance.b3 import Job, run_desflow, run_simpy
from tests.conformance.custom_resources import SimPyTwoWaiting, TwoWaiting
from tests.conformance.harness import (
    RecordingEgress,
    at,
    run_builder,
    run_low_level,
    run_yaml,
    run_yaml_text,
    times,
)


def _machine(sim_id):
    return SimParameter(
        sim_id=sim_id,
        resources={"machine": CapacityConfig(current_cap=1, max_cap=1)},
    )


def _reference(holder_at, hold, waiter_at, patience, until):
    """Plain SimPy: when the waiter reneged or started, by its own clock."""
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=1)
    outcome = []

    def holder():
        with machine.request() as req:
            yield req
            yield env.timeout(hold)

    def waiter():
        with machine.request() as req:
            result = yield req | env.timeout(patience)
            if req not in result:
                outcome.append(("reneged", env.now))
                return
            outcome.append(("started", env.now))
            yield env.timeout(1)

    def source():
        # The holder is started first, then the waiter, at their own times
        for when, proc in sorted(
            [(holder_at, holder), (waiter_at, waiter)], key=lambda p: p[0]
        ):
            if when > env.now:
                yield env.timeout(when - env.now)
            env.process(proc())

    env.process(source())
    env.run(until=until)
    return outcome


def _low_level(sim_id, holder_at, hold, waiter_at, patience, until, stats=None):
    def start(env):
        machine = DynamicResource(env, sim_id, "machine")

        def holder():
            with machine.request() as req:
                yield req
                yield env.timeout(hold)

        def waiter():
            env.publish_event("w-1", {"status": "queued"}, task="waiter", instance=0)
            env.stats.task_queued("waiter", 0)
            with machine.request() as req:
                result = yield req | env.timeout(patience)
                if req not in result:
                    env.publish_event(
                        "w-1", {"status": "reneged"}, task="waiter", instance=0
                    )
                    env.stats.task_reneged("waiter", 0)
                    return
                env.publish_event(
                    "w-1", {"status": "started"}, task="waiter", instance=0
                )
                yield env.timeout(1.0)

        def source():
            for when, proc in sorted(
                [(holder_at, holder), (waiter_at, waiter)], key=lambda p: p[0]
            ):
                if when > env.now:
                    yield env.timeout(when - env.now)
                env.process(proc())

        env.process(source())

    return run_low_level(_machine(sim_id), start, until, stats)


def _builder(sim_id, holder_at, hold, waiter_at, patience, until, stats=None):
    app = SimulationContext(sim_id, factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1)
    app.add_service("hold", dist="constant", value=hold)
    app.add_service("quick", dist="constant", value=1.0)
    if stats:
        app.with_stats(stats)

    @app.task(service_id="hold", resource_id="machine", name="holder")
    def holder(task_id):
        return {"event": "held"}

    @app.task(
        service_id="quick", resource_id="machine", name="waiter", patience=patience
    )
    def waiter(task_id):
        return {"event": "served"}

    def script(context):
        for when, task in sorted(
            [(holder_at, "holder"), (waiter_at, "waiter")], key=lambda p: p[0]
        ):
            if when > context.env.now:
                yield context.env.timeout(when - context.env.now)
            body = holder if task == "holder" else waiter
            context.spawn(body(context.next_instance(task)))

    app.add_process(script)
    return run_builder(app, until), app


def _waiter_outcome(records):
    reneged = [t for t, _key in at(records, "reneged")]
    if reneged:
        return [("reneged", reneged[0])]
    starts = [
        (t, key) for t, key in at(records, "started") if not key.startswith("holder")
    ]
    return [("started", starts[-1][0])]


def test_c7_reneging():
    """The waiter arrives at 3 with a patience of 3 and reneges at 6."""
    expected = _reference(1, 10, 3, 3, 7)
    assert expected == [("reneged", 6)]
    low, _env = _low_level("C7", 1.0, 10.0, 3.0, 3.0, 7)
    builder, _app = _builder("C7", 1.0, 10.0, 3.0, 3.0, 7)
    yaml, _app = run_yaml("patience_renege.yaml")
    for records in (low, builder, yaml):
        assert _waiter_outcome(records) == expected


def _reference_first_at_a_tie(n):
    """`n` holders serve 6 on `n` units from 0. `n` waiters ask at 3 with a patience of
    3, which ends at 6, the instant the units are given back. The waiter's timeout was
    made at 3, before the holders' release at 6 grants the requests, so at 6 the
    patience comes first and `req in result` is false: every waiter reneges."""
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=n)
    outcome = []

    def holder():
        with machine.request() as req:
            yield req
            yield env.timeout(6)

    def waiter():
        yield env.timeout(3)
        with machine.request() as req:
            result = yield req | env.timeout(3)
            outcome.append(("reneged" if req not in result else "started", env.now))

    for make in (holder, waiter):
        for _ in range(n):
            env.process(make())
    env.run(until=12)
    return outcome, machine.count


TIE_FIRST = """
simulation: {sim_id: C7tie, factor: 0}
resources:
  machine: {current_cap: %(n)d, max_cap: %(n)d}
services:
  hold: {dist: constant, value: 6}
  quick: {dist: constant, value: 1}
arrivals:
  holders: {replay: {path: holders.csv}, spawn: holder}
  waiters: {replay: {path: waiters.csv}, spawn: waiter}
tasks:
  holder:
    service: hold
    resource: machine
    payload: {event: held}
  waiter:
    %(body)s
    patience: 3
    payload: {event: served}
run: {until: 12}
"""


@pytest.mark.parametrize("n", [1, 3, 10])
@pytest.mark.parametrize("kind", ["resource", "wait_branch"])
def test_patience_first_at_a_tie_reneges(tmp_path, n, kind):
    """A patience that ends at the instant its unit is granted, when the patience's
    event comes first, reneges, as SimPy's `req in result` is false for a request
    granted after the condition was processed. The same holds when the task waits for
    its unit in a `wait` branch (register row I5)."""
    expected, _count = _reference_first_at_a_tie(n)
    assert expected == [("reneged", 6)] * n

    app = SimulationContext("C7tie", factor=0.0)
    app.add_resource("machine", current_cap=n, max_cap=n)
    app.add_service("hold", dist="constant", value=6)
    app.add_service("quick", dist="constant", value=1)
    options = (
        {"resource_id": "machine"}
        if kind == "resource"
        else {"wait": {"request": "machine"}}
    )

    @app.task(service_id="hold", resource_id="machine", name="holder")
    def holder(task_id):
        return {"event": "held"}

    @app.task(service_id="quick", patience=3, name="waiter", **options)
    def waiter(task_id):
        return {"event": "served"}

    def script(context):
        for i in range(n):
            context.spawn(holder(0, entity=Entity(f"h-{i}", "h")))
        yield context.env.timeout(3)
        for i in range(n):
            context.spawn(waiter(0, entity=Entity(f"w-{i}", "w")))

    app.add_process(script)
    records = run_builder(app, 12)
    assert at(records, "reneged") == [(6.0, f"w-{i}") for i in range(n)]
    assert at(records, "started") == [(0.0, f"h-{i}") for i in range(n)]
    assert app.get_resource("machine").count == 0

    body = (
        "resource: machine\n    service: quick"
        if kind == "resource"
        else "wait: {request: machine}\n    service: quick"
    )
    outcome = run_yaml_text(
        tmp_path,
        TIE_FIRST % {"n": n, "body": body},
        {"holders": [0] * n, "waiters": [3] * n},
    )
    assert outcome.error is None
    assert [t for t, _key in at(outcome.records, "reneged")] == [6.0] * n
    assert outcome.app_or_env.get_resource("machine").count == 0


def test_c7_reneging_tie_timeout_scheduled_before_the_grant():
    """Holder and waiter arrive together, the holder first: the waiter's patience
    timeout is scheduled before the holder's service timeout, so at the shared
    instant the patience wins and the waiter reneges, as in SimPy."""
    expected = _reference(0, 6, 0, 6, 8)
    assert expected == [("reneged", 6)]
    low, _env = _low_level("C7tie", 0.0, 6.0, 0.0, 6.0, 8)
    builder, _app = _builder("C7tie", 0.0, 6.0, 0.0, 6.0, 8)
    yaml, _app = run_yaml("patience_tie_reneged.yaml")
    assert _waiter_outcome(low) == expected
    assert _waiter_outcome(builder) == expected
    assert _waiter_outcome(yaml) == _reference(10, 6, 10, 6, 18)


def test_c7_reneged_request_leaves_the_queue():
    """A waiter that reneges at 3 leaves the queue at 3: over 8 seconds the mean
    queue length is 2 / 8 = 0.25, and no waiting record is kept."""
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=1)
    changes = []

    def holder():
        with machine.request() as req:
            yield req
            yield env.timeout(6)

    def waiter():
        yield env.timeout(1)
        with machine.request() as req:
            changes.append((env.now, len(machine.queue)))
            yield req | env.timeout(2)
        changes.append((env.now, len(machine.queue)))

    env.process(holder())
    env.process(waiter())
    env.run(until=8)
    area = sum(
        q * (t2 - t1) for (t1, q), (t2, _q) in zip(changes, changes[1:] + [(8, 0)])
    )
    assert area / 8 == 0.25

    name = "{}.stats.resources.machine.queue_length_avg".format
    _low, low_env = _low_level("C7q", 0.0, 6.0, 1.0, 2.0, 8, stats="basic")
    _builder_records, app = _builder("C7q", 0.0, 6.0, 1.0, 2.0, 8, stats="basic")
    for stats, sim_id in ((low_env.stats, "C7q"), (app.env.stats, "C7q")):
        assert stats.readings()[name(sim_id)] == 0.25
        assert stats.readings()["C7q.stats.tasks.waiter.reneged"] == 1
        assert ("waiter", 0) not in stats._in_flight
    _yaml, yaml_app = run_yaml("patience_queue.yaml")
    assert yaml_app.env.stats.readings()[name("C7q")] == 0.25


def _reference_balk():
    """Plain SimPy: a customer that balks checks first and never requests."""
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=1)
    balked, requests = [], []

    def customer(skip):
        if skip:
            balked.append(env.now)
            return
        with machine.request() as req:
            requests.append(env.now)
            yield req
            yield env.timeout(1)

    def source():
        yield env.timeout(5)
        env.process(customer(skip=True))

    env.process(source())
    env.run(until=7)
    return balked, requests


def test_c7_balking():
    """The skipper balks at 5: one balked event, no queued event, no request, and
    a count of one."""
    balked, requests = _reference_balk()
    assert (balked, requests) == ([5], [])

    built = {}

    def start(env):
        built["machine"] = DynamicResource(env, "C7balk", "machine")

        def customer():
            # The balk check comes before any request, so none is made
            env.publish_event("s-1", {"status": "balked"}, task="maybe", instance=0)
            env.stats.task_balked("maybe", 0)
            yield env.timeout(0)

        def source():
            yield env.timeout(5.0)
            env.process(customer())

        env.process(source())

    low, low_env = run_low_level(_machine("C7balk"), start, 7, stats="basic")

    app = SimulationContext("C7balk", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1)
    app.add_service("quick", dist="constant", value=1.0)
    app.with_stats("basic")

    @app.task(
        service_id="quick", resource_id="machine", name="maybe", balk="attr.skip == 1"
    )
    def maybe(task_id):
        return {"event": "served"}

    def script(context):
        yield context.env.timeout(5.0)
        context.spawn(maybe(0, entity=Entity("s-1", "skippers", {"skip": 1})))

    app.add_process(script)
    builder = run_builder(app, 7)
    yaml, yaml_app = run_yaml("balk.yaml")

    runs = (
        (low, low_env, built["machine"]),
        (builder, app.env, app.get_resource("machine")),
        (yaml, yaml_app.env, yaml_app.get_resource("machine")),
    )
    for records, env, machine in runs:
        assert [t for t, _key in at(records, "balked")] == [5.0]
        assert at(records, "queued") == []
        assert env.stats.readings()["C7balk.stats.tasks.maybe.balked"] == 1
        assert machine.queue == [] and machine.in_use == 0


# Patience with preemption: the patience counts waiting time only. A low job waits
# 10 s for the machine, is served from 10 and preempted at 13 by a high job that
# holds it for 5 s; it queues again with the patience it has left, so with a
# patience of 12 it reneges at 15, and with 20 it resumes at 18.


def _reference_preempted(patience):
    env = simpy.Environment()
    machine = simpy.PreemptiveResource(env, capacity=1)
    trace = []

    def job(name, priority, service):
        with machine.request(priority=priority) as req:
            yield req
            trace.append((env.now, name, "started"))
            yield env.timeout(service)
            trace.append((env.now, name, "finished"))

    def low():
        left, remaining, started = patience, 8.0, False
        while True:
            try:
                with machine.request(priority=2) as req:
                    since = env.now
                    result = yield req | env.timeout(left)
                    if req not in result:
                        trace.append((env.now, "low", "reneged"))
                        return
                    left -= env.now - since
                    trace.append((env.now, "low", "resumed" if started else "started"))
                    started, served_from = True, env.now
                    yield env.timeout(remaining)
                    trace.append((env.now, "low", "finished"))
                    return
            except simpy.Interrupt:
                remaining -= env.now - served_from
                trace.append((env.now, "low", "preempted"))

    def high():
        yield env.timeout(13)
        yield from job("high", 0, 5)

    env.process(job("first", 1, 10))
    env.process(low())
    env.process(high())
    env.run(until=40)
    return trace


def _named(records):
    names = {"firsts": "first", "lows": "low", "highs": "high"}
    out = []
    for r in records:
        if r["stream_type"] != "event":
            continue
        status = r["value"].get("status", "finished")
        if status != "queued":
            out.append((r["sim_ts"], names[r["key"].split("-")[0]], status))
    return out


def _builder_preempted(patience):
    app = SimulationContext("C7p", factor=0.0)
    app.add_resource(
        "machine", current_cap=1, max_cap=1, queue="priority", preempt=True
    )
    app.add_service("first", dist="constant", value=10)
    app.add_service("work", dist="constant", value=8)
    app.add_service("rush", dist="constant", value=5)
    app.add_arrival_replay("firsts", [{"time": 0}])
    app.add_arrival_replay("lows", [{"time": 0}])
    app.add_arrival_replay("highs", [{"time": 13}])

    @app.task(service_id="first", resource_id="machine", priority=1)
    def first_job(task_id):
        return {"event": "done"}

    @app.task(
        service_id="work",
        resource_id="machine",
        priority=2,
        patience=patience,
        on_interrupt="resume",
    )
    def low_job(task_id):
        return {"event": "done"}

    @app.task(service_id="rush", resource_id="machine", priority=0)
    def high_job(task_id):
        return {"event": "done"}

    for arrival, task in (
        ("firsts", first_job),
        ("lows", low_job),
        ("highs", high_job),
    ):

        def loop(context, arrival=arrival, task=task):
            while True:
                entity = yield context.wait_for_arrival(arrival)
                context.spawn(task(context.next_instance(task.__name__), entity=entity))

        app.arrival_loop(arrival)(loop)
    return run_builder(app, 40)


def _low_level_preempted(patience):
    trace = []

    def start(env):
        machine = DynamicResource(env, "C7p", "machine", queue="priority", preempt=True)

        def job(name, priority, service):
            with machine.request(priority=priority) as req:
                yield req
                trace.append((env.now, name, "started"))
                yield env.timeout(service)
                trace.append((env.now, name, "finished"))

        def low():
            left, remaining, started = patience, 8.0, False
            while True:
                try:
                    with machine.request(priority=2) as req:
                        since = env.now
                        result = yield req | env.timeout(left)
                        if req not in result:
                            trace.append((env.now, "low", "reneged"))
                            return
                        left -= env.now - since
                        trace.append(
                            (env.now, "low", "resumed" if started else "started")
                        )
                        started, served_from = True, env.now
                        yield env.timeout(remaining)
                        trace.append((env.now, "low", "finished"))
                        return
                except simpy.Interrupt:
                    remaining -= env.now - served_from
                    trace.append((env.now, "low", "preempted"))

        def high():
            yield env.timeout(13)
            yield from job("high", 0, 5)

        env.process(job("first", 1, 10))
        env.process(low())
        env.process(high())

    run_low_level(_machine("C7p"), start, 40)
    return trace


def test_c7_patience_pauses_during_service(monkeypatch):
    """With a patience of 12 the preempted low job has 2 s left and reneges at 15;
    with 20 it has 10 s left and resumes at 18, when the high job ends."""
    for patience, ending in (
        (12, (15, "low", "reneged")),
        (20, (18, "low", "resumed")),
    ):
        expected = sorted(_reference_preempted(patience))
        assert ending in expected
        assert sorted(_low_level_preempted(patience)) == expected
        assert sorted(_named(_builder_preempted(patience))) == expected
        monkeypatch.setenv("PATIENCE", str(patience))
        assert sorted(_named(run_yaml("c7_patience_preempted.yaml")[0])) == expected


# ------------------------------------------------------------ a limited line


class _RaisingList(list):
    """A put queue that refuses an append when it holds `maxlen` requests, as
    SimPy's `SortedQueue` does, in the order of arrival."""

    def __init__(self, maxlen):
        super().__init__()
        self.maxlen = maxlen

    def append(self, item):
        if len(self) >= self.maxlen:
            raise RuntimeError("Cannot append event. Queue is full.")
        super().append(item)


def _fifo_limited(limit, capacity=1):
    class Limited(simpy.Resource):
        PutQueue = staticmethod(functools.partial(_RaisingList, maxlen=limit))

    return lambda env: Limited(env, capacity=capacity)


def _priority_limited(limit):
    class Limited(simpy.PreemptiveResource):
        PutQueue = staticmethod(functools.partial(SortedQueue, maxlen=limit))

    return lambda env: Limited(env, capacity=1)


def test_c7_queue_limit(tmp_path):
    """A resource refuses a request when `queue_limit` requests wait, before it is
    granted or preempts, as SimPy's `SortedQueue(maxlen)` does: a first in, first out
    line of two, a line of one that refuses a request that would preempt, a limit of
    0 that refuses every request, a place freed when a waiting task reneges (it cancels
    its request), and a line whose waiting requests count after the capacity is
    lowered below the busy units. A task balks; a low-level `request()` raises SimPy's `RuntimeError`."""
    fifo = [
        Job("a", 0, 5),
        Job("b", 1, 3),
        Job("c", 2, 2),
        Job("d", 3, 1),
        Job("e", 5.5, 1),
    ]
    preempting = [
        Job("a", 0, 10, priority=2),
        Job("b", 1, 2, priority=3),
        Job("c", 2, 1, priority=1),
    ]
    renege = [
        Job("a", 0, 10),
        Job("b", 1, 3, patience=2),
        Job("c", 2, 1),
        Job("d", 4, 1),
    ]
    cases = {
        "fifo line of two": (
            {"current_cap": 1, "max_cap": 1, "queue_limit": 2},
            _fifo_limited(2),
            fifo,
        ),
        "preempting request": (
            {
                "current_cap": 1,
                "max_cap": 1,
                "queue": "priority",
                "preempt": True,
                "queue_limit": 1,
            },
            _priority_limited(1),
            preempting,
        ),
        "limit 0": (
            {"current_cap": 1, "max_cap": 1, "queue_limit": 0},
            _fifo_limited(0),
            [Job("a", 0, 5), Job("b", 1, 1)],
        ),
        "a renege frees a place": (
            {"current_cap": 1, "max_cap": 1, "queue_limit": 1},
            _fifo_limited(1),
            renege,
        ),
        "lowered capacity": (
            {
                "current_cap": 2,
                "max_cap": 2,
                "queue_limit": 1,
                "lower": (1, 1),
            },
            _fifo_limited(1, capacity=2),
            [Job("a", 0, 10), Job("b", 0, 10), Job("c", 2, 1), Job("d", 3, 1)],
        ),
    }
    for name, (spec, make, jobs) in cases.items():
        folder = tmp_path / name.replace(" ", "_")
        folder.mkdir()
        reference = run_simpy(make, jobs, 20, lower=spec.get("lower"))
        statuses = [status for _t, _n, status in reference.trace]
        assert "balked" in statuses, name
        for result in run_desflow(folder, spec, jobs, 20):
            assert result.trace == reference.trace, name
    # The line of one refused the preempting request, and the holder kept its unit
    trace = run_simpy(cases["preempting request"][1], preempting, 20).trace
    assert (2, "c", "balked") in trace and "preempted" not in [s for *_, s in trace]


def _reference_two_resources():
    """Plain SimPy: a job takes a unit of `first` (three units), then asks `second`
    (one unit, a line of one); the third job finds the line full and gives back its
    unit of `first`."""
    env = simpy.Environment()
    first = simpy.Resource(env, capacity=3)
    second = _fifo_limited(1)(env)
    trace = []

    def job(name, at):
        yield env.timeout(at)
        with first.request() as outer:
            yield outer
            try:
                inner = second.request()
            except RuntimeError:
                trace.append((env.now, name, "balked", first.count))
                return
            with inner:
                yield inner
                trace.append((env.now, name, "started", first.count))
                yield env.timeout(5)
                trace.append((env.now, name, "finished", first.count))

    for name, when in (("j1", 0), ("j2", 1), ("j3", 2)):
        env.process(job(name, when))
    env.run(until=20)
    return trace


def test_c7_queue_limit_on_the_second_of_two_resources(tmp_path):
    """A task whose second resource refuses it balks and gives back the unit of the
    first: after the run the first resource has no user."""
    expected = _reference_two_resources()
    assert [(t, n, s) for t, n, s, _c in expected][0] == (0, "j1", "started")
    assert (2, "j3", "balked", 3) in expected
    assert (5, "j1", "finished", 2) in expected

    def trace(records, app):
        return [
            (float(r["sim_ts"]), r["key"].rsplit("-", 1)[0], r["value"]["status"])
            for r in records
            if r["stream_type"] == "event"
            and r["value"].get("status") in ("started", "balked")
        ]

    names = [(t, n, s) for t, n, s, _c in expected if s != "finished"]
    app = SimulationContext("C7s", factor=0.0)
    app.add_resource("first", current_cap=3, max_cap=3)
    app.add_resource("second", current_cap=1, max_cap=1, queue_limit=1)
    app.add_service("work", dist="constant", value=5)

    @app.task(service_id="work", resource_id=["first", "second"], name="job")
    def job(task_id):
        return {"event": "done"}

    def script(context):
        for when, name in ((0, "j1"), (1, "j2"), (2, "j3")):
            yield context.env.timeout(when - context.env.now)
            context.spawn(
                job(context.next_instance("job"), entity=Entity(f"{name}-1", name))
            )

    app.add_process(script)
    egress = RecordingEgress()
    app.add_egress(egress)
    app.run(until=20)
    assert trace(egress.records, app) == names
    assert app.get_resource("first").in_use == 0

    folder = tmp_path / "blueprint"
    folder.mkdir()
    for name, when in (("j1", 0), ("j2", 1), ("j3", 2)):
        (folder / f"{name}.csv").write_text(f"time\n{when}\n")
    (folder / "model.yaml").write_text(
        """
simulation: {sim_id: C7s, factor: 0}
resources:
  first: {current_cap: 3, max_cap: 3}
  second: {current_cap: 1, max_cap: 1, queue_limit: 1}
services:
  work: {dist: constant, value: 5}
arrivals:
  j1: {replay: {path: j1.csv}, spawn: job}
  j2: {replay: {path: j2.csv}, spawn: job}
  j3: {replay: {path: j3.csv}, spawn: job}
tasks:
  job:
    service: work
    resource: [first, second]
    payload: {event: done}
run: {until: 20}
"""
    )
    app = SimulationContext.from_yaml(folder / "model.yaml")
    egress = RecordingEgress()
    app.add_egress(egress)
    app.run()
    assert trace(egress.records, app) == names
    assert app.get_resource("first").in_use == 0


def _reference_wait_branches():
    """Plain SimPy: a holder keeps a machine whose line holds one request; a task
    joins the line; an `any` and an `all` condition each build a request that the
    full line refuses, and the condition's other event, built first, is dropped."""
    env = simpy.Environment()
    machine = _fifo_limited(1)(env)
    seen = {}

    def holder():
        with machine.request() as req:
            yield req
            yield env.timeout(10)

    def joiner():
        yield env.timeout(1)
        with machine.request() as req:
            yield req
            yield env.timeout(2)
        seen["joiner"] = env.now

    def refused(name, at, build):
        yield env.timeout(at)
        events = []
        try:
            build(events)
        except RuntimeError as error:
            for event in events:
                if hasattr(event, "cancel"):
                    event.cancel()
            seen[name] = (env.now, str(error))

    def any_branches(events):
        events.append(env.timeout(5))
        events.append(machine.request())

    def all_branches(events):
        events.append(env.timeout(1))
        events.append(machine.request())

    env.process(holder())
    env.process(joiner())
    env.process(refused("any_waiter", 2, any_branches))
    env.process(refused("all_waiter", 3, all_branches))
    env.run(until=20)
    return seen


def test_c7_queue_limit_fails_a_request_wait_branch():
    """A request wait branch on a full line fails the wait in an `any` and an `all`
    wait at once, with SimPy's message, and leaves the line and the units as they
    were; the task that had joined the line is served after the holder."""
    expected = _reference_wait_branches()
    reason = "Cannot append event. Queue is full."
    assert expected == {
        "any_waiter": (2, reason),
        "all_waiter": (3, reason),
        "joiner": 12,
    }

    def seen(records):
        return {
            "any_waiter": (times(records, "any_waiter", "failed")[0], reason)
            if times(records, "any_waiter", "failed")
            else None,
            "all_waiter": (times(records, "all_waiter", "failed")[0], reason)
            if times(records, "all_waiter", "failed")
            else None,
            "joiner": times(records, "joiner")[0],
        }

    records, app = run_yaml("c7_queue_limit_wait.yaml")
    assert seen(records) == expected
    reasons = {
        r["task"]: r["value"]["reason"]
        for r in records
        if r["stream_type"] == "event" and r["value"].get("status") == "failed"
    }
    assert reasons == {"any_waiter": reason, "all_waiter": reason}
    assert app.get_resource("machine").in_use == 0
    assert app.get_resource("machine").queue == []

    app = SimulationContext("C7w", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1, queue_limit=1)
    app.add_service("hold", dist="constant", value=10)
    app.add_service("work", dist="constant", value=2)

    @app.task(service_id="hold", resource_id="machine", name="holder")
    def holder(task_id):
        return {"event": "held"}

    @app.task(wait={"request": "machine"}, service_id="work", name="joiner")
    def joiner(task_id):
        return {"event": "done"}

    @app.task(
        wait={"any": {"unit": {"request": "machine"}, "tired": {"timeout": 5}}},
        name="any_waiter",
    )
    def any_waiter(task_id):
        return {"event": "done"}

    @app.task(
        wait={"all": {"pause": {"timeout": 1}, "unit": {"request": "machine"}}},
        name="all_waiter",
    )
    def all_waiter(task_id):
        return {"event": "done"}

    def script(context):
        for when, task in ((0, holder), (1, joiner), (2, any_waiter), (3, all_waiter)):
            yield context.env.timeout(when - context.env.now)
            context.spawn(task(context.next_instance(task.__name__)))

    app.add_process(script)
    assert seen(run_builder(app, 20, on_unhandled_failure="continue")) == expected
    assert app.get_resource("machine").in_use == 0


def _statuses(records, job):
    """The statuses a job published, in order; a finished event has none."""
    return [
        r["value"].get("status", "finished")
        for r in records
        if r["stream_type"] == "event" and r["task"] == job
    ]


def test_c7_a_type_with_its_own_queue_limit_balks_before_queued(tmp_path):
    """A custom type whose own `PutQueue` has a `maxlen` refuses the task before it
    publishes `queued`, as a `queue_limit` does; the line of two is full at 3."""
    spec = {
        "current_cap": 1,
        "max_cap": 1,
        "queue": "priority",
        "type": TwoWaiting,
    }
    jobs = [
        Job("a", 0, 5, priority=1),
        Job("b", 1, 3, priority=2),
        Job("c", 2, 2, priority=0),
        Job("d", 3, 1, priority=1),
    ]
    reference = run_simpy(lambda env: SimPyTwoWaiting(env, capacity=1), jobs, 20)
    assert (3, "d", "balked") in reference.trace
    for result in run_desflow(tmp_path, spec, jobs, 20)[1:]:
        assert result.trace == reference.trace
        assert _statuses(result.records, "d") == ["balked"]
        assert _statuses(result.records, "c")[0] == "queued"


KEEP_BALK = """
simulation: {sim_id: T4, factor: 0}
resources:
  machine: {}
  other: {}
services:
  short: {dist: constant, value: 1}
arrivals:
  jobs: {replay: {path: jobs.csv}, spawn: pick}
tasks:
  pick:
    service: short
    resource: machine
    keep: [machine]
    next: pack
    payload: {event: picked}
  pack:
    service: short
    resource: other
    balk: t >= 0
    payload: {event: packed}
run: {until: 10}
"""


def test_c7_a_balk_condition_gives_back_the_unit_the_task_kept(tmp_path):
    """A task that balks on its `balk:` expression gives back the unit the task
    before it kept for it, as a refusal by a full line does."""
    outcome = run_yaml_text(tmp_path, KEEP_BALK, {"jobs": [1]})
    assert outcome.error is None
    assert outcome.app_or_env.get_resource("machine").in_use == 0
    assert times(outcome.records, "pack", "balked") == [2.0]


PREEMPTED_LINE = """
simulation: {sim_id: T3, factor: 0}
resources:
  machine: {queue: priority, preempt: true, queue_limit: 1}
services:
  long: {dist: constant, value: 5}
arrivals:
  los: {replay: {path: los.csv}, spawn: lo}
tasks:
  lo:
    service: long
    wait: {request: machine}
    payload: {event: done}
run: {until: 20}
"""


def test_c7_a_preempted_request_wait_balks_when_the_line_is_full(tmp_path):
    """A task whose `request` wait branch won a unit and lost it to a preemption asks
    again on resume; the line is full by then, so the task balks and the run goes
    on, as a `resource:` task does."""

    def prepare(app):
        def script(context):
            env = context.env
            yield env.timeout(1)
            machine = context.get_resource("machine")
            first = machine.request(priority=-1)  # preempts the task's unit
            machine.request(priority=4)  # takes the one place in the line
            yield env.timeout(1)
            machine.release(first)

        app.add_process(script)

    outcome = run_yaml_text(tmp_path, PREEMPTED_LINE, {"los": [0]}, prepare)
    assert outcome.error is None
    assert _statuses(outcome.records, "lo") == [
        "queued",
        "started",
        "preempted",
        "balked",
    ]


def test_c7_a_preempted_seize_balks_when_the_line_is_full(tmp_path):
    """B-U2: a `seize` unit lost to a preemption is asked for again on resume; the
    line is full by then, so the task balks, as a `request` wait branch does."""

    def prepare(app):
        def script(context):
            env = context.env
            yield env.timeout(1)
            machine = context.get_resource("machine")
            first = machine.request(priority=-1)  # preempts the task's unit
            machine.request(priority=4)  # takes the one place in the line
            yield env.timeout(1)
            machine.release(first)

        app.add_process(script)

    text = PREEMPTED_LINE.replace("wait: {request: machine}", "seize: machine")
    outcome = run_yaml_text(tmp_path, text, {"los": [0]}, prepare)
    assert outcome.error is None
    assert _statuses(outcome.records, "lo") == [
        "queued",
        "started",
        "preempted",
        "balked",
    ]


PREEMPTED_WAIT = """
simulation: {sim_id: T3, factor: 0}
resources:
  machine: {queue: priority, preempt: true}
services:
  long: {dist: constant, value: 5}
arrivals:
  los: {replay: {path: los.csv}, spawn: lo}
tasks:
  lo:
    service: long
    wait: {request: machine}
    PATIENCE
    payload: {event: done}
run: {until: 20}
"""


def _preempt_then_interrupt(app):
    def script(context):
        env = context.env
        yield env.timeout(1)
        machine = context.get_resource("machine")
        first = machine.request(priority=-1)  # preempts the task's unit
        yield env.timeout(1)
        machine.queue[0].proc.interrupt("x")  # the task waits to ask again
        yield env.timeout(1)
        machine.release(first)

    app.add_process(script)


def test_c7_a_interrupt_while_a_preempted_request_waits_leaves_no_stale_request(
    tmp_path,
):
    """An interrupt while the task waits for the unit it asks for again takes that
    request out of the line, so the resume asks once and the task finishes."""
    text = PREEMPTED_WAIT.replace("    PATIENCE\n", "")
    outcome = run_yaml_text(tmp_path, text, {"los": [0]}, _preempt_then_interrupt)
    assert outcome.error is None
    machine = outcome.app_or_env.get_resource("machine")
    assert len(machine.queue) == 0
    assert machine.in_use == 0
    assert _statuses(outcome.records, "lo")[-2:] == ["resumed", "finished"]


def test_c7_a_preempted_request_wait_targets_the_resource_while_it_asks_again(
    tmp_path,
):
    """While the task waits for the unit it asked for again, its process target
    names the resource, not the service the preemption stopped."""
    seen = []

    def watch(app):
        def script(context):
            env = context.env
            yield env.timeout(1)
            machine = context.get_resource("machine")
            first = machine.request(priority=-1)  # preempts the task's unit
            yield env.timeout(1)
            task = machine.queue[0].proc._generator.gi_frame.f_locals["self"]
            seen.append(task._target)  # what `process.target` reads
            machine.release(first)

        app.add_process(script)

    text = PREEMPTED_WAIT.replace("    PATIENCE\n", "")
    outcome = run_yaml_text(tmp_path, text, {"los": [0]}, watch)
    assert outcome.error is None
    assert seen == ["resource.machine"]


def test_c7_a_preempted_request_wait_reneges_when_the_patience_runs_out(tmp_path):
    """The request a task makes again after a preemption has the task's patience,
    as a `resource:` request has: the task reneges 4 after it asked again."""

    def prepare(app):
        def script(context):
            yield context.env.timeout(1)
            context.get_resource("machine").request(priority=-1)

        app.add_process(script)

    text = PREEMPTED_WAIT.replace("PATIENCE", "patience: 4")
    outcome = run_yaml_text(tmp_path, text, {"los": [0]}, prepare)
    assert outcome.error is None
    assert times(outcome.records, "lo", "reneged") == [5.0]
    assert len(outcome.app_or_env.get_resource("machine").queue) == 0


def test_c7_patience_that_evaluates_to_text_is_read_as_a_number(tmp_path):
    """A patience expression that gives text, such as an attribute written "2", is
    converted to a number as it was before numbers were kept as written."""
    text = (
        "simulation: {sim_id: C7, factor: 0}\n"
        "resources: {machine: {current_cap: 1, max_cap: 1}}\n"
        "services: {hold: {dist: constant, value: 5}}\n"
        "arrivals:\n"
        "  a: {replay: {path: a.csv}, spawn: hold, attributes: {p: '2'}}\n"
        "tasks:\n"
        "  hold: {service: hold, resource: machine, patience: attr.p,"
        " payload: {event: done}}\n"
        "run: {until: 20}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"a": [0, 1]})
    assert outcome.error is None, outcome.error


def _child_value_reference(n):
    """`n` workers hold a machine of capacity `n` until 5. Each parent waits for a child
    that asks for the machine with a patience of 2. A child that gives up returns
    without a value, which the parent reads as `ok` with no value; a child that is
    served returns a value."""
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=n)
    seen = []

    def holder():
        with machine.request() as req:
            yield req
            yield env.timeout(5)

    def child():
        req = machine.request()
        result = yield req | env.timeout(2)
        if req not in result:
            req.cancel()
            return
        yield env.timeout(1)
        machine.release(req)
        return "served"

    def parent(name, start):
        yield env.timeout(start)
        value = yield env.process(child())
        seen.append((name, "no value" if value is None else "value"))

    for _ in range(n):
        env.process(holder())
    for i in range(n):
        env.process(parent(f"early-{i}", 0))
    for i in range(n):
        env.process(parent(f"late-{i}", 6))
    env.run(until=30)
    return sorted(seen)


CHILD_VALUE = """
simulation: {sim_id: C7value, factor: 0}
resources: {machine: {current_cap: %(n)d, max_cap: %(n)d}}
services: {hold: {dist: constant, value: 5}, one: {dist: constant, value: 1}}
arrivals:
  holders: {replay: {path: holders.csv}, spawn: holder}
  early: {replay: {path: early.csv}, spawn: early}
  late: {replay: {path: late.csv}, spawn: late}
tasks:
  holder: {service: hold, resource: machine, payload: {event: held}}
  early: &parent
    wait: {task: line}
    payload: {event: parent}
    next:
      when:
        - {condition: "wait.line.ok and not wait.line.value", task: gave_up}
      default: got_it
  late: *parent
  line: {service: one, resource: machine, patience: 2, payload: {event: served}}
  gave_up: {payload: {event: no_value}}
  got_it: {payload: {event: value}}
run: {until: 30}
"""


@pytest.mark.parametrize("n", [1, 3, 10])
def test_child_that_reneged_reports_simpy_value(tmp_path, n):
    """A child flow whose request reneged ends with `ok` true and no value, as a SimPy
    child that returns without a value does, and a served child has a value
    (register row I6, the `wait: {task: ...}` form)."""
    expected = _child_value_reference(n)
    assert [kind for _name, kind in expected] == ["no value"] * n + ["value"] * n

    outcome = run_yaml_text(
        tmp_path,
        CHILD_VALUE % {"n": n},
        {"holders": [0] * n, "early": [0] * n, "late": [6] * n},
    )
    assert outcome.error is None
    ended = sorted(
        r["value"]["event"]
        for r in outcome.records
        if r["stream_type"] == "event" and r["task"] in ("gave_up", "got_it")
    )
    assert ended == ["no_value"] * n + ["value"] * n


def _reference_timeout_branch_at_a_tie(n):
    """`n` waiters yield `(t | s) | patience` with the patience made first and both
    ending at 5. The patience is processed first and triggers the condition, but `t`
    is processed before the condition is, and a SimPy condition builds its value when
    it is processed, so `t in result` is true: the waiter is not reneged."""
    env = simpy.Environment()
    outcome = []

    def waiter():
        signal = env.event()
        patience = env.timeout(5)
        limit = env.timeout(5)
        result = yield (limit | signal) | patience
        outcome.append("done" if limit in result else "reneged")

    for _ in range(n):
        env.process(waiter())
    env.run(until=12)
    return outcome


@pytest.mark.parametrize("n", [1, 3, 10])
def test_patience_does_not_beat_a_timeout_branch_processed_first(tmp_path, n):
    """A patience and a `timeout` branch of an `any` wait that end at one instant: the
    timeout is processed before the wait's condition, so it is in the condition's value
    and the wait is done, as in SimPy. It does not renege (register row I5)."""
    assert _reference_timeout_branch_at_a_tie(n) == ["done"] * n

    app = SimulationContext("C7leaf", factor=0.0)
    app.add_signal("s")

    @app.task(
        wait={"any": {"t": {"timeout": 5}, "s": {"signal": "s"}}},
        patience=5,
        name="W",
    )
    def waiter(task_id):
        return {"event": "done"}

    def script(context):
        for i in range(n):
            context.spawn(waiter(0, entity=Entity(f"w-{i}", "w")))
        yield context.env.timeout(0)

    app.add_process(script)
    records = run_builder(app, 12)
    assert [r["value"].get("status") for r in records if r["task"] == "W"].count(
        "reneged"
    ) == 0
    assert at(records, "reneged") == []

    text = (
        "simulation: {sim_id: C7leaf, factor: 0}\n"
        "signals: {s: {}}\n"
        "arrivals: {w: {replay: {path: w.csv}, spawn: W}}\n"
        "tasks:\n"
        "  W: {wait: {any: {t: {timeout: 5}, s: {signal: s}}}, patience: 5, "
        "payload: {event: done}}\n"
        "run: {until: 12}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"w": [0] * n})
    assert outcome.error is None
    assert at(outcome.records, "reneged") == []
    assert len([r for r in outcome.records if r["value"].get("event") == "done"]) == n


def _reference_get_at_a_tie(n):
    """A putter made first puts `n` items at 3. `n` waiters ask for an item at 0 with a
    patience of 3. The putter's step comes first, so the get is granted before the
    waiter resumes, but after the condition was processed: `get in result` is false,
    the waiter reneges and the item is lost."""
    env = simpy.Environment()
    box = simpy.Store(env)
    outcome = []

    def putter():
        yield env.timeout(3)
        for i in range(n):
            yield box.put(i)

    def waiter():
        get = box.get()
        result = yield get | env.timeout(3)
        outcome.append("got" if get in result else "reneged")

    env.process(putter())
    for _ in range(n):
        env.process(waiter())
    env.run(until=12)
    return outcome, len(box.items)


@pytest.mark.parametrize("n", [1, 3, 10])
def test_patience_first_at_a_tie_reneges_a_get(tmp_path, n):
    """A patience on a `get` that ends at the instant the item is put, with the put made
    first, reneges when the get is granted after the patience's condition was
    processed, as SimPy's `get in result` is false (register row I5)."""
    outcome, items = _reference_get_at_a_tie(n)
    assert outcome == ["reneged"] * n
    text = (
        "simulation: {sim_id: C7get, factor: 0}\n"
        "stores: {s: {current_cap: 20, max_cap: 20}}\n"
        "arrivals:\n"
        "  putters: {replay: {path: putters.csv}, spawn: putter}\n"
        "  waiters: {replay: {path: waiters.csv}, spawn: waiter}\n"
        "tasks:\n"
        "  putter: {put: {store: s}, payload: {event: put}}\n"
        "  waiter: {get: {store: s}, patience: 3, payload: {event: got}}\n"
        "run: {until: 12}\n"
    )
    outcome_yaml = run_yaml_text(
        tmp_path, text, {"putters": [3] * n, "waiters": [0] * n}
    )
    assert outcome_yaml.error is None
    assert [t for t, _k in at(outcome_yaml.records, "reneged")] == [3.0] * n
    assert len(outcome_yaml.app_or_env.get_store("s").items) == items


def _reference_rerequest_at_a_tie(n):
    """`2n` units. `n` jobs of priority 1 serve 10 and `n` holders of priority 0 hold
    their units until 5; the holders' timeouts were made at 0. `n` urgent jobs take the
    jobs' units at 2 and keep them. A preempted job asks again with a patience of 3,
    which ends at 5. The holders' release is processed before the patience's condition
    resumes the job, and grants the request, but after the condition was processed:
    `again in result` is false and the job reneges."""
    env = simpy.Environment()
    machine = simpy.PreemptiveResource(env, capacity=2 * n)
    outcome = []

    def job():
        req = machine.request(priority=1)
        yield req
        try:
            yield env.timeout(10)
        except simpy.Interrupt:
            again = machine.request(priority=1)
            result = yield again | env.timeout(3)
            outcome.append("reneged" if again not in result else "started")
            if again not in result:
                again.cancel()
            return
        machine.release(req)

    def holder():
        with machine.request(priority=0) as req:
            yield req
            yield env.timeout(5)

    def urgent():
        yield env.timeout(2)
        with machine.request(priority=0) as req:
            yield req
            yield env.timeout(100)

    for make in (holder, job, urgent):
        for _ in range(n):
            env.process(make())
    env.run(until=30)
    return outcome


@pytest.mark.parametrize("n", [1, 3, 10])
def test_patience_first_at_a_tie_reneges_a_request_asked_again(tmp_path, n):
    """A preempted `wait` request branch that asks again, with a patience that ends at
    the instant a unit is given back, reneges when the patience's condition was
    processed first, although the request is granted before the task resumes
    (register row I5)."""
    assert _reference_rerequest_at_a_tie(n) == ["reneged"] * n
    text = (
        "simulation: {sim_id: C7again, factor: 0}\n"
        f"resources: {{machine: {{current_cap: {2 * n}, max_cap: {2 * n}, "
        "queue: priority, preempt: true}}\n"
        "services: {long: {dist: constant, value: 10}, five: {dist: constant, value: 5}, "
        "huge: {dist: constant, value: 100}}\n"
        "arrivals:\n"
        "  holders: {replay: {path: holders.csv}, spawn: holder}\n"
        "  jobs: {replay: {path: jobs.csv}, spawn: job}\n"
        "  urgent: {replay: {path: urgent.csv}, spawn: rush}\n"
        "tasks:\n"
        "  holder: {resource: machine, service: five, priority: 0, payload: {event: held}}\n"
        "  job: {wait: {request: machine}, service: long, priority: 1, patience: 3, "
        "payload: {event: done}}\n"
        "  rush: {resource: machine, service: huge, priority: 0, payload: {event: rush}}\n"
        "run: {until: 30}\n"
    )
    outcome = run_yaml_text(
        tmp_path, text, {"holders": [0] * n, "jobs": [0] * n, "urgent": [2] * n}
    )
    assert outcome.error is None
    assert [t for t, _k in at(outcome.records, "reneged")] == [5.0] * n
