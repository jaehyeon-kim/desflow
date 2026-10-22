"""C5 a put or get that is not waited for (`wait: false`).

A plain SimPy process that creates a put or get event and does not yield it sets the
behaviour: the process goes on at once, and the request stays queued until there is
room, an item or an amount. A task with `wait: false` must match, in the low-level
API, the builder and YAML.
"""

import simpy

from desflow import (
    ContainerConfig,
    DynamicContainer,
    SimParameter,
)
from desflow.core.context import SimulationContext
from tests.conformance.harness import (
    probe,
    probe_env,
    run_builder,
    run_low_level,
    run_yaml_text,
    times,
)

UNTIL = 8
READ_AT = [2, 4]


def _levels(setup):
    """Levels of a tank at the times in READ_AT, in plain SimPy. `setup(env, tank)`
    starts the processes."""
    env = simpy.Environment()
    tank = simpy.Container(env, capacity=10, init=setup.init)
    finished = []
    seen = []

    def read():
        for moment in READ_AT:
            yield env.timeout(moment - env.now)
            seen.append((moment, tank.level))

    def request():
        yield env.timeout(1)
        setup.first(tank)  # not yielded: the process goes on at once
        finished.append(env.now)

    def other():
        yield env.timeout(3)
        yield setup.second(tank)

    env.process(request())
    env.process(other())
    env.process(read())
    env.run(until=UNTIL)
    return finished, seen


class _Put:
    init = 8
    first = staticmethod(lambda tank: tank.put(5))
    second = staticmethod(lambda tank: tank.get(4))


class _Get:
    init = 2
    first = staticmethod(lambda tank: tank.get(5))
    second = staticmethod(lambda tank: tank.put(4))


def _tank_model(case):
    """The four APIs run the case: one task asks at 1 without waiting, one acts at 3.
    Returns (finish time of the first, levels read)."""
    first_kind = "put" if case is _Put else "get"
    second_kind = "get" if case is _Put else "put"
    amounts = (5, 4)

    def low_level():
        read, readings = probe_env(READ_AT, lambda env: env.tank.level)
        finished = []

        def start(env):
            env.tank = DynamicContainer(env, "C5", "tank")

            def first():
                yield env.timeout(1)
                getattr(env.tank, first_kind)(amounts[0])
                finished.append(env.now)

            def second():
                yield env.timeout(3)
                yield getattr(env.tank, second_kind)(amounts[1])

            env.process(first())
            env.process(second())
            env.process(read(env))

        params = SimParameter(
            sim_id="C5",
            containers={
                "tank": ContainerConfig(current_cap=10, max_cap=10, level=case.init)
            },
        )
        run_low_level(params, start, UNTIL)
        return finished, readings

    def builder():
        app = SimulationContext("C5", factor=0.0)
        app.add_container("tank", current_cap=10, max_cap=10, level=case.init)

        @app.task(
            name="first",
            **{first_kind: {"container": "tank", "amount": amounts[0], "wait": False}},
        )
        def first(task_id):
            return {"event": "went_on"}

        @app.task(
            name="second",
            **{second_kind: {"container": "tank", "amount": amounts[1]}},
        )
        def second(task_id):
            return {"event": "done"}

        def script(context):
            yield context.env.timeout(1)
            context.spawn(first(0))
            yield context.env.timeout(2)
            context.spawn(second(0))

        app.add_process(script)
        read, readings = probe(READ_AT, lambda c: c.get_container("tank").level)
        app.add_process(read)
        records = run_builder(app, UNTIL)
        return times(records, "first"), readings

    def yaml_model(tmp_path):
        text = f"""
simulation: {{sim_id: C5, factor: 0}}
containers:
  tank: {{current_cap: 10, max_cap: 10, level: {case.init}}}
arrivals:
  firsts: {{replay: {{path: firsts.csv}}, spawn: first}}
  seconds: {{replay: {{path: seconds.csv}}, spawn: second}}
tasks:
  first:
    {first_kind}: {{container: tank, amount: {amounts[0]}, wait: false}}
    payload: {{event: went_on}}
  second:
    {second_kind}: {{container: tank, amount: {amounts[1]}}}
    payload: {{event: done}}
run: {{until: {UNTIL}}}
"""
        read, readings = probe(READ_AT, lambda c: c.get_container("tank").level)
        outcome = run_yaml_text(
            tmp_path,
            text,
            {"firsts": [1], "seconds": [3]},
            prepare=lambda app: app.add_process(read),
        )
        assert outcome.error is None
        return times(outcome.records, "first"), readings

    return low_level, builder, yaml_model


def _check(case, tmp_path, expected_levels):
    finished, seen = _levels(case)
    assert finished == [1]
    assert seen == expected_levels
    low_level, builder, yaml_model = _tank_model(case)
    for got_finished, readings in (low_level(), builder(), yaml_model(tmp_path)):
        assert got_finished == [1.0]
        assert readings == expected_levels


def test_c5_put_not_waited_for(tmp_path):
    """A tank-truck puts 5 into a tank of 10 holding 8 and does not wait: it finishes
    at 1 while the put stays queued. The take of 4 at 3 makes room, so the put is
    granted then: the level is 8 at 2 and 9 at 4."""
    _check(_Put, tmp_path, [(2, 8), (4, 9)])


def test_c5_get_not_waited_for(tmp_path):
    """A get of 5 from a tank holding 2 does not wait: the task finishes at 1. The put
    of 4 at 3 lets the get take 5 from the 6 held: the level is 2 at 2 and 1 at 4."""
    _check(_Get, tmp_path, [(2, 2), (4, 1)])


def test_c5_no_wait_puts_are_served_in_order(tmp_path):
    """Three tasks put into a full store of 1 at 1, 2 and 3 without waiting; takes at
    4, 5 and 6 return the stored item and then the first two puts in order, and the take at 6
    lets the third put in."""
    env = simpy.Environment()
    store = simpy.Store(env, capacity=1)
    store.items.append({"n": 0})
    taken = []

    def stash(at, n):
        yield env.timeout(at)
        store.put({"n": n})

    def take(at):
        yield env.timeout(at)
        taken.append((yield store.get()))

    for at, n in ((1, 1), (2, 2), (3, 3)):
        env.process(stash(at, n))
    for at in (4, 5, 6):
        env.process(take(at))
    env.run(until=UNTIL)
    assert taken == [{"n": 0}, {"n": 1}, {"n": 2}]
    assert store.items == [{"n": 3}]

    app = SimulationContext("C5", factor=0.0)
    app.add_store("shelf", current_cap=1, max_cap=1, items=[{"n": 0}])
    got = []

    def make_stash(n):
        @app.task(name=f"stash{n}", put={"store": "shelf", "wait": False})
        def stash_task(task_id):
            return {"n": n}

        return stash_task

    @app.task(name="take", get={"store": "shelf"})
    def take_task(task_id, item):
        got.append(item)
        return {"event": "took"}

    stashes = [make_stash(n) for n in (1, 2, 3)]

    def script(context):
        for at, stash_task in zip((1, 2, 3), stashes):
            yield context.env.timeout(at - context.env.now)
            context.spawn(stash_task(0))
        for at in (4, 5, 6):
            yield context.env.timeout(at - context.env.now)
            context.spawn(take_task(at))

    app.add_process(script)
    run_builder(app, UNTIL)
    assert got == [{"n": 0}, {"n": 1}, {"n": 2}]
    assert app.get_store("shelf").items == [{"n": 3}]

    text = f"""
simulation: {{sim_id: C5, factor: 0}}
stores:
  shelf: {{current_cap: 1, max_cap: 1, items: [{{n: 0}}]}}
arrivals:
  s1: {{replay: {{path: s1.csv}}, spawn: stash1}}
  s2: {{replay: {{path: s2.csv}}, spawn: stash2}}
  s3: {{replay: {{path: s3.csv}}, spawn: stash3}}
  takes: {{replay: {{path: takes.csv}}, spawn: take}}
tasks:
  stash1:
    put: {{store: shelf, wait: false}}
    payload: {{n: 1}}
  stash2:
    put: {{store: shelf, wait: false}}
    payload: {{n: 2}}
  stash3:
    put: {{store: shelf, wait: false}}
    payload: {{n: 3}}
  take:
    get: {{store: shelf}}
    item_field: item
    payload: {{event: took}}
run: {{until: {UNTIL}}}
"""
    outcome = run_yaml_text(
        tmp_path, text, {"s1": [1], "s2": [2], "s3": [3], "takes": [4, 5, 6]}
    )
    assert outcome.error is None
    took = [
        r["value"]["item"]
        for r in outcome.records
        if r["stream_type"] == "event"
        and r["task"] == "take"
        and isinstance(r["value"], dict)
        and "item" in r["value"]
    ]
    assert took == [{"n": 0}, {"n": 1}, {"n": 2}]


def test_c5_no_wait_put_ends_with_the_run(tmp_path):
    """A put nobody makes room for is dropped when the run ends: the task finished at
    once, and the run ends at `until` with the item not stored."""
    text = """
simulation: {sim_id: C5, factor: 0}
stores:
  shelf: {current_cap: 1, max_cap: 1, items: [{n: 0}]}
arrivals:
  puts: {replay: {path: puts.csv}, spawn: stash}
tasks:
  stash:
    put: {store: shelf, wait: false}
    payload: {n: 1}
run: {until: 5}
"""
    outcome = run_yaml_text(tmp_path, text, {"puts": [1]})
    assert outcome.error is None
    assert times(outcome.records, "stash") == [1.0]
    assert outcome.app_or_env.get_store("shelf").items == [{"n": 0}]
