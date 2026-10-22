"""C5 custom store and container types: a subclass of `StoreBase` or `ContainerBase`
behaves as its SimPy twin does.

Each rule in `custom_types.py` is a mixin shared by the SimPy class and the desflow
class. One scenario runs on the SimPy twin and on the desflow type in the low-level
API, the builder and YAML, and the results compare. A rule that does not call
`super()` still lets the context see the store change.
"""

import simpy

from desflow import (
    CapacityConfig,
    ContainerConfig,
    DynamicContainer,
    DynamicStore,
    SimParameter,
)
from desflow.core.context import SimulationContext
from tests.conformance import custom_types as custom
from tests.conformance.harness import run_builder, run_low_level, run_yaml_text

UNTIL = 10
MODULE = "tests.conformance.custom_types"


def _taken(records):
    """`(get id, time, item)` for every get task that finished, from its event."""
    found = []
    for record in records:
        value = record.get("value")
        if (
            record["stream_type"] == "event"
            and isinstance(value, dict)
            and "item" in value
        ):
            found.append((record["task"], record["sim_ts"], value["item"]))
    return sorted(found)


# ------------------------------------------------------------ stores


def _simpy_store(cls, puts, gets):
    env = simpy.Environment()
    store = cls(env)
    out = []

    def put(at, item):
        yield env.timeout(at)
        yield store.put({"v": item})

    def get(at, name):
        yield env.timeout(at)
        item = yield store.get()
        out.append((name, env.now, item["v"]))

    for at, item in puts:
        env.process(put(at, item))
    for at, name in gets:
        env.process(get(at, name))
    env.run(until=UNTIL)
    return sorted(out)


def _low_level_store(cls, puts, gets):
    def start(env):
        store = DynamicStore(env, "C5", "dock", type=cls)

        def put(at, item):
            yield env.timeout(at)
            yield store.put({"v": item})

        def get(at, name):
            yield env.timeout(at)
            item = yield store.get()
            env.publish_event(name, {"item": item["v"]}, task=name, instance=0)

        for at, item in puts:
            env.process(put(at, item))
        for at, name in gets:
            env.process(get(at, name))

    params = SimParameter(sim_id="C5", stores={"dock": CapacityConfig()})
    records, _env = run_low_level(params, start, UNTIL)
    return _taken(records)


def _builder_store(cls, puts, gets):
    app = SimulationContext("C5", factor=0.0)
    app.add_store("dock", type=cls)
    plan = []
    for index, (at, item) in enumerate(puts):

        @app.task(name=f"put{index}", put={"store": "dock"})
        def put(task_id, item=item):
            return {"v": item}

        plan.append((at, put))
    for at, name in gets:

        @app.task(name=name, get={"store": "dock"})
        def get(task_id, item):
            return {"item": item["v"]}

        plan.append((at, get))

    def script(context):
        for at, task in sorted(plan, key=lambda pair: pair[0]):
            yield context.env.timeout(at - context.env.now)
            context.spawn(task(0))

    app.add_process(script)
    return _taken(run_builder(app, UNTIL))


def _yaml_store(tmp_path, cls, puts, gets):
    arrivals, tasks, files = "", "", {}
    for index, (at, item) in enumerate(puts):
        arrivals += (
            f"  p{index}: {{replay: {{path: p{index}.csv}}, spawn: put{index}}}\n"
        )
        tasks += (
            f"  put{index}:\n    put: {{store: dock}}\n    payload: {{v: {item}}}\n"
        )
        files[f"p{index}"] = [at]
    for at, name in gets:
        arrivals += f"  {name}s: {{replay: {{path: {name}s.csv}}, spawn: {name}}}\n"
        tasks += (
            f"  {name}:\n    get: {{store: dock}}\n    item_field: item\n"
            "    payload: {}\n"
        )
        files[f"{name}s"] = [at]
    text = f"""
simulation: {{sim_id: C5, factor: 0}}
stores:
  dock: {{type: !python {MODULE}.{cls.__name__}}}
arrivals:
{arrivals}tasks:
{tasks}run: {{until: {UNTIL}}}
"""
    outcome = run_yaml_text(tmp_path, text, files)
    assert outcome.error is None
    return [
        (task, ts, item["v"] if isinstance(item, dict) else item)
        for task, ts, item in _taken(outcome.records)
    ]


def _folder(tmp_path, name):
    folder = tmp_path / name
    folder.mkdir()
    return folder


def _check_store(tmp_path, simpy_cls, cls, puts, gets):
    expected = _simpy_store(simpy_cls, puts, gets)
    assert _low_level_store(cls, puts, gets) == expected
    assert _builder_store(cls, puts, gets) == expected
    assert _yaml_store(_folder(tmp_path, cls.__name__), cls, puts, gets) == expected
    return expected


def test_c5_custom_store_type(tmp_path):
    """A `_do_get` that pops the newest item and never calls `super()`: puts at 1, 2
    and 3 are taken newest first by gets at 4, 5 and 6."""
    puts = [(1, 1), (2, 2), (3, 3)]
    gets = [(4, "g1"), (5, "g2"), (6, "g3")]
    expected = _check_store(tmp_path, custom.SimPyDock, custom.Dock, puts, gets)
    assert expected == [("g1", 4, 3), ("g2", 5, 2), ("g3", 6, 1)]


def test_c5_custom_queue_types(tmp_path):
    """A `GetQueue` whose `append` puts the newest first: gets waiting from 1, 2 and 3
    are served newest first by puts at 4, 5 and 6."""
    puts = [(4, 1), (5, 2), (6, 3)]
    gets = [(1, "g1"), (2, "g2"), (3, "g3")]
    expected = _check_store(tmp_path, custom.SimPyStack, custom.Stack, puts, gets)
    assert expected == [("g1", 6, 3), ("g2", 5, 2), ("g3", 4, 1)]


def test_c5_custom_event_classes():
    """A `put` bound to a `StorePut` subclass stamps `env.now` on each event, in
    SimPy, the low-level API and the builder."""
    env = simpy.Environment()
    store = custom.SimPyStampingDock(env)
    stamps = []

    def producer(env, store):
        for at in (2, 5):
            yield env.timeout(at - env.now)
            stamps.append(store.put("x").stamp)

    env.process(producer(env, store))
    env.run(until=UNTIL)
    assert stamps == [2, 5]

    low = []

    def start(env):
        dock = DynamicStore(env, "C5", "dock", type=custom.StampingDock)

        def producer():
            for at in (2, 5):
                yield env.timeout(at - env.now)
                low.append(dock.put("x").stamp)

        env.process(producer())

    params = SimParameter(sim_id="C5", stores={"dock": CapacityConfig()})
    run_low_level(params, start, UNTIL)
    assert low == [2, 5]

    app = SimulationContext("C5", factor=0.0)
    app.add_store("dock", type=custom.StampingDock)
    built = []

    def script(context):
        for at in (2, 5):
            yield context.env.timeout(at - context.env.now)
            built.append(context.get_store("dock").put("x").stamp)

    app.add_process(script)
    run_builder(app, UNTIL)
    assert built == [2, 5]


def test_c5_custom_store_type_statistics():
    """A store whose `_do_get` does not call `super()` still reports its level: items
    held are 0 to 1, 1 to 2 and 2 to 3, then 1 after the get at 3, so the average
    over 4 s is 1."""
    app = SimulationContext("C5", factor=0.0)
    app.add_store("dock", type=custom.Dock)
    app.with_stats("basic", interval=1000)

    @app.task(name="put", put={"store": "dock"})
    def put(task_id):
        return {"v": task_id}

    @app.task(name="get", get={"store": "dock"})
    def get(task_id, item):
        return {"item": item["v"]}

    def script(context):
        yield context.env.timeout(1)
        context.spawn(put(1))
        yield context.env.timeout(1)
        context.spawn(put(2))
        yield context.env.timeout(1)
        context.spawn(get(0))

    app.add_process(script)
    run_builder(app, 4)
    readings = app.env.stats.readings()
    assert readings["C5.stats.stores.dock.level"] == 1
    assert readings["C5.stats.stores.dock.level_avg"] == 1.0


# ------------------------------------------------------------ containers


def _simpy_tank(cls):
    env = simpy.Environment()
    tank = cls(env, capacity=10, init=0)
    out = []
    error = []

    def get(at, name, amount):
        yield env.timeout(at)
        yield tank.get(amount)
        out.append((name, env.now))

    def put(at, amount):
        yield env.timeout(at)
        yield tank.put(amount)

    env.process(get(1, "big", 5))
    env.process(get(2, "small", 2))
    env.process(put(3, 3))
    try:
        env.run(until=UNTIL)
    except RuntimeError as exc:
        error.append(exc)
    return out, error


def _low_level_tank(cls):
    out = []

    def start(env):
        tank = DynamicContainer(env, "C5", "tank", type=cls)

        def get(at, name, amount):
            yield env.timeout(at)
            yield tank.get(amount)
            out.append((name, env.now))

        def put(at, amount):
            yield env.timeout(at)
            yield tank.put(amount)

        env.process(get(1, "big", 5))
        env.process(get(2, "small", 2))
        env.process(put(3, 3))

    params = SimParameter(
        sim_id="C5", containers={"tank": ContainerConfig(current_cap=10, max_cap=10)}
    )
    outcome = run_low_level(params, start, UNTIL, raises=True)
    return out, outcome.error


def _builder_tank(cls):
    app = SimulationContext("C5", factor=0.0)
    app.add_container("tank", current_cap=10, max_cap=10, type=cls)

    @app.task(name="big", get={"container": "tank", "amount": 5})
    def big(task_id):
        return {"event": "done"}

    @app.task(name="small", get={"container": "tank", "amount": 2})
    def small(task_id):
        return {"event": "done"}

    @app.task(name="fill", put={"container": "tank", "amount": 3})
    def fill(task_id):
        return {"event": "done"}

    def script(context):
        yield context.env.timeout(1)
        context.spawn(big(0))
        yield context.env.timeout(1)
        context.spawn(small(0))
        yield context.env.timeout(1)
        context.spawn(fill(0))

    app.add_process(script)
    outcome = run_builder(app, UNTIL, raises=True)
    done = [
        (r["task"], r["sim_ts"])
        for r in outcome.records
        if r["stream_type"] == "event"
        and isinstance(r["value"], dict)
        and r["value"].get("event") == "done"
        and r["task"] in ("big", "small")
    ]
    return done, outcome.error


def _yaml_tank(tmp_path, cls):
    text = f"""
simulation: {{sim_id: C5, factor: 0}}
containers:
  tank: {{current_cap: 10, max_cap: 10, type: !python {MODULE}.{cls.__name__}}}
arrivals:
  bigs: {{replay: {{path: bigs.csv}}, spawn: big}}
  smalls: {{replay: {{path: smalls.csv}}, spawn: small}}
  fills: {{replay: {{path: fills.csv}}, spawn: fill}}
tasks:
  big:
    get: {{container: tank, amount: 5}}
    payload: {{event: done}}
  small:
    get: {{container: tank, amount: 2}}
    payload: {{event: done}}
  fill:
    put: {{container: tank, amount: 3}}
    payload: {{event: done}}
run: {{until: {UNTIL}}}
"""
    outcome = run_yaml_text(tmp_path, text, {"bigs": [1], "smalls": [2], "fills": [3]})
    done = [
        (r["task"], r["sim_ts"])
        for r in outcome.records
        if r["stream_type"] == "event"
        and isinstance(r["value"], dict)
        and r["value"].get("event") == "done"
        and r["task"] in ("big", "small")
    ]
    return done, outcome.error


def test_c5_custom_container_type(tmp_path):
    """A `_do_get` that returns True when the amount is not there serves the small get
    of 2 at 3 around the big get of 5 that does not fit; the big one keeps waiting."""
    expected, error = _simpy_tank(custom.SimPyTank)
    assert expected == [("small", 3)] and error == []
    for got, failed in (
        _low_level_tank(custom.Tank),
        _builder_tank(custom.Tank),
        _yaml_tank(_folder(tmp_path, "tank"), custom.Tank),
    ):
        assert got == expected
        assert failed is None


def test_c5_custom_container_broken_queue(tmp_path):
    """A waiting line whose `pop` ignores its index breaks SimPy's check that the
    request served is the request removed, and SimPy raises `RuntimeError`. The desflow
    class raises the same error from the same place."""
    _out, error = _simpy_tank(custom.SimPyBrokenTank)
    assert len(error) == 1 and "invariant" in str(error[0])
    for _got, failed in (
        _low_level_tank(custom.BrokenTank),
        _builder_tank(custom.BrokenTank),
        _yaml_tank(_folder(tmp_path, "broken"), custom.BrokenTank),
    ):
        assert isinstance(failed, RuntimeError)
        assert "invariant" in str(failed)
