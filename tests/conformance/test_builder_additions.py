"""Builder forms that arrive in 1.0.0: V1, V6, A7, I6 and M3.

Each test runs a SimPy 4.1.2 program and the Builder model that does the same, and
compares what happened and when. The counts of entities are 1, 3 and 10 where an
order could show a difference.

V6: a filter that names a parameter `entity` reads the entity, as a SimPy filter
reads the process that asks. V1: the time a task takes comes from the entity. A7: a
task starts a number of entities, the number computed. I6: a reneging or balking
task goes to a task. M3: a task function that raises goes to the task `on_fail`
names.
"""

import numpy as np
import pytest
import simpy

from desflow.core.context import SimulationContext
from tests.conformance.harness import run_builder

SIZES = [1, 3, 10]


def _rows(count):
    return [{"time": float(n), "n": n} for n in range(1, count + 1)]


def _events(records, task, field):
    """(time, entity counter, payload field) of every finished event of a task."""
    return [
        (r["sim_ts"], int(r["key"].rsplit("-", 1)[1]), r["value"][field])
        for r in records
        if r["stream_type"] == "event"
        and r["task"] == task
        and isinstance(r["value"], dict)
        and field in r["value"]
    ]


# ---------------------------------------------------------------------------
# V6: a filter that names `entity`
# ---------------------------------------------------------------------------
def _area(n):
    return "N" if n % 2 == 0 else "S"


def _supply_order(count):
    return sorted(range(1, count + 1), key=lambda k: (_area(k) != "N", k))


def _reference_filter(count):
    env = simpy.Environment()
    parcels = simpy.FilterStore(env)
    served = []

    def user(n):
        yield env.timeout(n)
        area = _area(n)
        parcel = yield parcels.get(lambda item: item["area"] == area)
        served.append((env.now, n, parcel["id"]))

    def supplier():
        # Parcels arrive after every user waits, the "N" ones first
        yield env.timeout(count + 1)
        for k in _supply_order(count):
            yield parcels.put({"area": _area(k), "id": f"p{k}"})
            yield env.timeout(1)

    for n in range(1, count + 1):
        env.process(user(n))
    env.process(supplier())
    env.run(until=3 * count + 5)
    return served


def _builder_filter(count, take_filter):
    app = SimulationContext("V6", factor=0.0)
    app.add_store("parcels", current_cap=count, max_cap=count)

    @app.task(name="take", get={"store": "parcels", "filter": take_filter})
    def take(task_id, item):
        return {"parcel": item["id"]}

    @app.task(name="stock", put={"store": "parcels"})
    def stock(task_id, entity):
        return {
            "area": _area(entity.attributes["n"]),
            "id": f"p{entity.attributes['n']}",
        }

    app.add_arrival_replay(
        "users",
        [{"time": float(n), "n": n, "area": _area(n)} for n in range(1, count + 1)],
        spawn="take",
    )
    app.add_arrival_replay(
        "supply",
        [
            {"time": float(count + 1 + i), "n": k}
            for i, k in enumerate(_supply_order(count))
        ],
        spawn="stock",
    )
    records = run_builder(app, 3 * count + 5)
    return [(t, n, parcel) for t, n, parcel in _events(records, "take", "parcel")]


@pytest.mark.parametrize("count", SIZES)
def test_filter_takes_the_entity_by_name(count):
    """A filter `lambda item, entity: ...` receives the entity by name and reads its
    attributes, and a filter of one parameter keeps its call with the item. Both
    serve the users the parcels SimPy's filter serves, at the same times."""
    reference = _reference_filter(count)
    assert len(reference) == count
    # The reference reaches the case: later users are served out of arrival order
    # once there are two areas to tell apart
    if count > 1:
        assert [n for _, n, _ in reference] != sorted(n for _, n, _ in reference)

    by_name = _builder_filter(
        count, lambda item, entity: item["area"] == entity.attributes["area"]
    )
    by_text = _builder_filter(count, "item.area == attr.area")

    # An entity's counter is the order its row arrives: key `users-<n>`
    assert [(t, parcel) for t, _n, parcel in by_name] == [
        (t, parcel) for t, _n, parcel in reference
    ]
    assert by_name == by_text


def test_filter_of_one_parameter_is_called_with_the_item():
    """A one-parameter filter, even one named `entity`, is called with the item, as
    it always was; only a filter with more parameters reads the entity."""
    seen = []

    def one(entity):
        seen.append(type(entity).__name__)
        return True

    app = SimulationContext("V6b", factor=0.0)
    app.add_store("parcels", current_cap=2, max_cap=2, items=[{"area": "N", "id": "p"}])

    @app.task(name="take", get={"store": "parcels", "filter": one})
    def take(task_id, item):
        return {"parcel": item["id"]}

    app.add_arrival_replay("users", [{"time": 1.0}], spawn="take")
    run_builder(app, 5)

    assert seen == ["dict"]


# ---------------------------------------------------------------------------
# V1: service_time from the entity
# ---------------------------------------------------------------------------
def _reference_service(count):
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=1)
    finished = []

    def job(n):
        yield env.timeout(0.5 * n)
        with machine.request() as req:
            yield req
            yield env.timeout(1.0 + n % 3)
        finished.append((env.now, n))

    for n in range(1, count + 1):
        env.process(job(n))
    env.run()
    return finished


def _builder_service(count, service_time):
    app = SimulationContext("V1", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1)

    @app.task(name="work", resource_id="machine", service_time=service_time)
    def work(task_id, entity):
        return {"n": entity.attributes["n"]}

    app.add_arrival_replay(
        "jobs",
        [{"time": 0.5 * n, "n": n} for n in range(1, count + 1)],
        spawn="work",
    )
    records = run_builder(app, 10 * count + 10)
    return [(t, n) for t, _k, n in _events(records, "work", "n")]


@pytest.mark.parametrize("count", SIZES)
def test_builder_service_time(count):
    """`service_time` takes a function of the entity, a duration text, an expression
    on the entity's attributes or a number, and the jobs finish when a SimPy machine
    shop finishes them, each holding the machine for the time computed from it."""
    reference = _reference_service(count)
    assert len(reference) == count
    # The jobs queue for the machine, so the finish times are not the arrival times
    assert any(t > 0.5 * n + 3 for t, n in reference) or count == 1

    assert (
        _builder_service(count, lambda entity: 1.0 + entity.attributes["n"] % 3)
        == reference
    )
    assert _builder_service(count, "1 + attr.n % 3") == reference
    # SimPy accepts a NumPy integer as a delay, so a draw from `rng.integers` works
    sim = simpy.Environment()
    sim.timeout(np.int64(3))
    sim.run()
    assert sim.now == 3
    assert _builder_service(count, lambda entity: np.int64(2)) == (
        _builder_service(count, 2)
    )
    assert _builder_service(count, np.int64(2)) == _builder_service(count, 2)

    constant = _builder_service(count, 2)
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=1)
    expected = []

    def job(n):
        yield env.timeout(0.5 * n)
        with machine.request() as req:
            yield req
            yield env.timeout(2)
        expected.append((env.now, n))

    for n in range(1, count + 1):
        env.process(job(n))
    env.run()
    assert constant == expected


def test_builder_service_time_refuses_what_it_cannot_run():
    app = SimulationContext("V1b", factor=0.0)
    app.add_service("s", dist="constant", value=1.0)
    with pytest.raises(ValueError, match="service_id or service_time, not both"):
        app.task(name="a", service_id="s", service_time=2)
    with pytest.raises(ValueError, match="service_time must be seconds"):
        app.task(name="b", service_time=[1])
    with pytest.raises(ValueError, match="service_time too"):
        app.task(name="c", resource_id="machine")


# ---------------------------------------------------------------------------
# A7: spawn with a count
# ---------------------------------------------------------------------------
def _reference_spawn(count, children):
    env = simpy.Environment()
    started = []

    def child(parent, k):
        started.append((env.now, parent, k))
        yield env.timeout(0)

    def parent(n):
        yield env.timeout(n)
        for k in range(1, children(n) + 1):
            env.process(child(n, k))

    for n in range(1, count + 1):
        env.process(parent(n))
    env.run()
    return started


def _builder_spawn(count, spawn_count, children):
    app = SimulationContext("A7", factor=0.0)

    @app.task(name="parent", spawn={"task": "child", "count": spawn_count})
    def parent(task_id, entity):
        return {"n": entity.attributes["n"]}

    @app.task(name="child")
    def child(task_id, entity):
        return {"parent": entity.attributes["parent"]}

    app.add_arrival_replay(
        "parents",
        [{"time": float(n), "n": n} for n in range(1, count + 1)],
        spawn="parent",
    )
    records = run_builder(app, count + 5)
    seen = {}
    out = []
    for t, _k, parent_key in _events(records, "child", "parent"):
        seen[parent_key] = seen.get(parent_key, 0) + 1
        out.append((t, parent_key, seen[parent_key]))
    return out, records


@pytest.mark.parametrize("count", SIZES)
def test_builder_spawn_count(count):
    """`spawn={"task": t, "count": n}` starts n entities of t when the task finishes,
    whether n is a number or a function of the parent entity (zero starts none),
    and each starts at the instant the parent finishes, as SimPy's processes do."""

    def children(n):
        return n % 4

    def keys(started):
        return sorted(started)

    reference = _reference_spawn(count, children)
    assert (sum(children(n) for n in range(1, count + 1)) > 0) and reference

    by_function, records = _builder_spawn(
        count, lambda entity: children(entity.attributes["n"]), children
    )
    # The child's payload is the parent's key, which the SimPy parent number maps to
    by_number = {(t, int(p.split("-")[1]), k) for t, p, k in by_function}
    assert keys(by_number) == keys(reference)

    fixed, _ = _builder_spawn(count, 2, lambda n: 2)
    assert len(fixed) == 2 * count


# ---------------------------------------------------------------------------
# I6: on_renege and on_balk
# ---------------------------------------------------------------------------
def _reference_leave(count, patience, limit):
    """One machine held for 5 seconds by each served customer. A customer who finds
    `limit` waiting balks; one who waits `patience` seconds reneges."""
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=1)
    outcome = []

    def customer(n):
        yield env.timeout(0.1 * n)
        if len(machine.queue) >= limit:
            outcome.append((env.now, n, "balked"))
            return
        with machine.request() as req:
            got = yield req | env.timeout(patience)
            if req not in got:
                outcome.append((env.now, n, "reneged"))
                return
            yield env.timeout(5)
            outcome.append((env.now, n, "served"))

    for n in range(1, count + 1):
        env.process(customer(n))
    env.run(until=100)
    return outcome


def _builder_leave(count, patience, limit):
    app = SimulationContext("I6", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1)
    app.add_service("hold", dist="constant", value=5.0)

    @app.task(
        name="serve",
        service_id="hold",
        resource_id="machine",
        patience=patience,
        balk=f"queue.machine >= {limit}",
        on_renege="gave_up",
        on_balk="turned_away",
    )
    def serve(task_id, entity):
        return {"outcome": "served", "n": entity.attributes["n"]}

    @app.task(name="gave_up")
    def gave_up(task_id, entity):
        return {"outcome": "reneged", "n": entity.attributes["n"]}

    @app.task(name="turned_away")
    def turned_away(task_id, entity):
        return {"outcome": "balked", "n": entity.attributes["n"]}

    app.add_arrival_replay(
        "customers",
        [{"time": 0.1 * n, "n": n} for n in range(1, count + 1)],
        spawn="serve",
    )
    records = run_builder(app, 100)
    return sorted(
        (round(r["sim_ts"], 6), r["value"]["n"], r["value"]["outcome"])
        for r in records
        if r["stream_type"] == "event"
        and isinstance(r["value"], dict)
        and "outcome" in r["value"]
    )


@pytest.mark.parametrize("count", SIZES)
def test_builder_on_renege_on_balk(count):
    """A task that reneges goes to `on_renege`, and one that balks goes to
    `on_balk`, at the instant SimPy's customer leaves: the instants and the order
    of the served, reneged and balked customers match the SimPy program."""
    patience, limit = 3.0, 2
    reference = sorted(
        (round(t, 6), n, kind)
        for t, n, kind in _reference_leave(count, patience, limit)
    )
    kinds = {kind for _, _, kind in reference}
    # The reference reaches each case once the line is long enough to need it
    if count == 10:
        assert kinds == {"served", "reneged", "balked"}

    built = _builder_leave(count, patience, limit)

    assert built == reference


def test_on_renege_and_on_balk_need_their_cause():
    app = SimulationContext("I6b", factor=0.0)
    with pytest.raises(ValueError, match="needs patience"):
        app.task(name="a", on_renege="b")
    with pytest.raises(ValueError, match="needs a balk or a resource"):
        app.task(name="c", on_balk="d")


# ---------------------------------------------------------------------------
# M3: on_fail without a fail condition
# ---------------------------------------------------------------------------
def _reference_raise(count):
    env = simpy.Environment()
    results = []

    def work(n):
        # The function of a desflow task runs as the task starts, then the service
        if n % 2 == 0:
            raise ValueError(f"bad {n}")
        yield env.timeout(1)
        results.append((env.now, n, "done"))

    def recover(n, reason):
        yield env.timeout(0)
        results.append((env.now, n, f"recovered: {reason}"))

    def job(n):
        yield env.timeout(n)
        try:
            yield env.process(work(n))
        except ValueError as error:
            yield env.process(recover(n, f"ValueError: {error}"))

    for n in range(1, count + 1):
        env.process(job(n))
    env.run()
    return sorted(results)


def _builder_raise(count):
    app = SimulationContext("M3", factor=0.0)
    app.add_service("one", dist="constant", value=1.0)

    @app.task(name="work", service_id="one", on_fail="recover")
    def work(task_id, entity):
        n = entity.attributes["n"]
        if n % 2 == 0:
            raise ValueError(f"bad {n}")
        return {"n": n, "result": "done"}

    @app.task(name="recover")
    def recover(task_id, entity, upstream=None):
        return {"n": entity.attributes["n"], "result": "recovered"}

    app.add_arrival_replay(
        "jobs", [{"time": float(n), "n": n} for n in range(1, count + 1)], spawn="work"
    )
    records = run_builder(app, count + 5)
    failed = {
        r["key"]: r["value"]["reason"]
        for r in records
        if r["stream_type"] == "event"
        and isinstance(r["value"], dict)
        and r["value"].get("status") == "failed"
    }
    done = sorted(
        (r["sim_ts"], r["value"]["n"], r["value"]["result"])
        for r in records
        if r["stream_type"] == "event"
        and isinstance(r["value"], dict)
        and r["value"].get("result") in ("done", "recovered")
    )
    return done, failed


@pytest.mark.parametrize("count", SIZES)
def test_builder_on_fail_catches_a_raise(count):
    """A task function that raises fails the task with the error as the reason and
    goes to `on_fail`, with no `fail` condition, as a SimPy process that catches the
    exception goes on to its recovery step: the same entities recover at the same
    instants, and the others finish."""
    reference = _reference_raise(count)
    expected = [
        (t, n, "recovered" if kind.startswith("recovered") else "done")
        for t, n, kind in reference
    ]
    if count > 1:
        assert any(kind.startswith("recovered") for _, _, kind in reference)

    done, failed = _builder_raise(count)

    assert done == expected
    assert {f"jobs-{n}" for n in range(1, count + 1) if n % 2 == 0} == set(failed)
    assert all(reason.startswith("ValueError: bad") for reason in failed.values())
