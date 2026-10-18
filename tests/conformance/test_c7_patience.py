"""C7 patience: reneging after a wait, and balking before joining.

Reneging: a holder takes the one machine and keeps it; a waiter with a patience
leaves the queue when the patience runs out before a unit is granted. When the
grant and the patience timeout fall at the same instant, SimPy's outcome depends
on which event was scheduled first, and desflow follows the same order in all
three APIs. Balking: a task whose balk condition holds never joins the queue, so
it creates no request and is counted once.
"""

import simpy

from desflow import (
    CapacityConfig,
    DynamicResource,
    Entity,
    SimParameter,
)
from desflow.core.context import SimulationContext
from tests.conformance.harness import at, run_builder, run_low_level, run_yaml


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
            yield req | env.timeout(patience)
            if not req.triggered:
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
                yield req | env.timeout(patience)
                if not req.triggered:
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


def test_c7_reneging_tie_timeout_scheduled_after_the_grant():
    """The holder's service ends at 6, scheduled at 0; the waiter's patience ends
    at 6, scheduled at 3. SimPy serves the waiter, and so does desflow."""
    expected = _reference(0, 6, 3, 3, 8)
    assert expected == [("started", 6)]
    low, _env = _low_level("C7tie", 0.0, 6.0, 3.0, 3.0, 8)
    builder, _app = _builder("C7tie", 0.0, 6.0, 3.0, 3.0, 8)
    # YAML arrivals start at their first gap, so the same tie is at 16
    yaml, _app = run_yaml("patience_tie_served.yaml")
    assert _waiter_outcome(low) == expected
    assert _waiter_outcome(builder) == expected
    assert _waiter_outcome(yaml) == [("started", 16.0)]


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
    assert _waiter_outcome(yaml) == [("reneged", 16.0)]


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
        assert machine.queue.items == [] and machine.in_use == 0
