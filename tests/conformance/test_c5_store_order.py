"""C5 equal-time puts and gets: a store without priority grants gets as simpy.Store.

Two orders wait for parcels. At 5, a courier puts two parcels and a third order asks
for one, all in the same step. SimPy's `Store` grants one waiting get each time it
is triggered, so the third order's request gives the first parcel to the first
order, and the second order is served only when the first put is processed.
desflow's store is a `FilterStore` underneath, so that a get can take a filter;
while no waiting get has a filter it must grant as `Store` does. The test reads,
in that step, which gets are triggered, and the time and item of every get.
"""

import simpy

from desflow import CapacityConfig, DynamicStore, SimParameter
from tests.conformance.harness import run_low_level


def _model(env, store, log):
    def order(n, at):
        yield env.timeout(at)
        item = yield store.get()
        log.append((env.now, f"order-{n}", item))

    def courier():
        yield env.timeout(5)
        store.put("parcel-1")
        store.put("parcel-2")
        third = store.get()
        # Which orders' gets are granted in this same step
        log.append(("granted", first_two[0].triggered, first_two[1].triggered))
        item = yield third
        log.append((env.now, "order-3", item))

    first_two = []

    def first(n):
        yield env.timeout(1)
        request = store.get()
        first_two.append(request)
        item = yield request
        log.append((env.now, f"order-{n}", item))

    env.process(first(1))
    env.process(first(2))
    env.process(courier())

    def late():
        yield env.timeout(6)
        store.put("parcel-3")

    env.process(late())


def _reference():
    env = simpy.Environment()
    log: list = []
    _model(env, simpy.Store(env, capacity=3), log)
    env.run(until=10)
    return log


def _low_level():
    log: list = []

    def start(env):
        _model(env, DynamicStore(env, "C5", "bin"), log)

    params = SimParameter(
        sim_id="C5", stores={"bin": CapacityConfig(current_cap=3, max_cap=3)}
    )
    run_low_level(params, start, 10)
    return log


def test_c5_equal_time_puts_and_gets_follow_simpy_store():
    """In the step at 5 only the first order's get is granted, as simpy.Store
    grants it; the second is served when the first put is processed, and the third
    order waits for the parcel of 6."""
    expected = _reference()
    assert expected[0] == ("granted", True, False)
    assert _low_level() == expected


# The same shop as tasks: each order and each parcel arrives from a replayed row and
# is one task, as YAML and the builder write it, and plain SimPy runs the same
# processes in the same order.

ROWS = {
    "early": [(1, {}), (1, {})],
    "parcels": [
        (5, {"name": "parcel-1"}),
        (5, {"name": "parcel-2"}),
        (6, {"name": "parcel-3"}),
    ],
    "late": [(5, {})],
}


def _reference_tasks():
    env = simpy.Environment()
    store = simpy.Store(env, capacity=3)
    taken = []

    def take(key):
        item = yield store.get()
        taken.append((env.now, key, item["name"]))

    def drop(fields):
        yield store.put(dict(fields))

    def rows(name, start):
        for number, (at, fields) in enumerate(ROWS[name], start=1):
            yield env.timeout(at - env.now)
            key = f"{name}-{number}"
            env.process(take(key) if start == "take" else drop(fields))

    env.process(rows("early", "take"))
    env.process(rows("parcels", "drop"))
    env.process(rows("late", "take"))
    env.run(until=10)
    return taken


def _taken(records):
    return [
        (r["sim_ts"], r["key"], r["value"]["parcel"]["name"])
        for r in records
        if r["stream_type"] == "event" and r["value"].get("event") == "taken"
    ]


def _builder_tasks():
    from desflow.core.context import SimulationContext
    from tests.conformance.harness import run_builder

    app = SimulationContext("C5", factor=0.0)
    app.add_store("parcels", current_cap=3, max_cap=3)
    for name, rows in ROWS.items():
        app.add_arrival_replay(name, [{"time": at, **fields} for at, fields in rows])

    @app.task(get={"store": "parcels"})
    def take(task_id, item):
        return {"event": "taken", "parcel": item}

    @app.task(put={"store": "parcels"})
    def drop(task_id, entity):
        return {"name": entity.attributes["name"]}

    for name, task in (("early", take), ("parcels", drop), ("late", take)):

        def loop(context, name=name, task=task):
            while True:
                entity = yield context.wait_for_arrival(name)
                context.spawn(task(context.next_instance(task.__name__), entity=entity))

        app.arrival_loop(name)(loop)

    return run_builder(app, 10)


def test_c5_equal_time_puts_and_gets_as_tasks():
    """In YAML and the builder each order takes the parcel, at the time, that the
    same processes on simpy.Store give it."""
    from tests.conformance.harness import run_yaml

    expected = _reference_tasks()
    assert [(t, k) for t, k, _ in expected] == [
        (5, "early-1"),
        (5, "early-2"),
        (6, "late-1"),
    ]
    for records in (_builder_tasks(), run_yaml("c5_store_order.yaml")[0]):
        assert _taken(records) == [(float(t), k, n) for t, k, n in expected]
