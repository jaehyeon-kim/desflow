"""C5 priority item storage: a store returns the lowest priority first.

A plain SimPy `PriorityStore` sets the order. The low-level `DynamicStore` takes
`PriorityItem`s directly; the builder and YAML give a task's `put` a priority and
wrap the payload, with a rising put order so equal priorities come out first in,
first out and payloads are never compared.
"""

import heapq

import simpy

from desflow import CapacityConfig, DynamicStore, Entity, SimParameter
from desflow.core.context import SimulationContext
from tests.conformance.harness import (
    run_builder,
    run_low_level,
    run_yaml,
    run_yaml_text,
)


def _reference_order():
    env = simpy.Environment()
    store = simpy.PriorityStore(env)
    out = []

    def flow():
        yield store.put(simpy.PriorityItem((5, 0), "low"))
        yield store.put(simpy.PriorityItem((1, 1), "high"))
        out.append((yield store.get()).item)
        out.append((yield store.get()).item)

    env.process(flow())
    env.run()
    return out


def _low_level_order():
    out = []

    def start(env):
        store = DynamicStore(env, "C5", "buffer", priority=True)

        def flow():
            yield store.put(simpy.PriorityItem((5, 0), "low"))
            yield store.put(simpy.PriorityItem((1, 1), "high"))
            out.append((yield store.get()).item)
            out.append((yield store.get()).item)

        env.process(flow())

    params = SimParameter(
        sim_id="C5", stores={"buffer": CapacityConfig(current_cap=10, max_cap=10)}
    )
    run_low_level(params, start, 5)
    return out


def _builder_order(priorities):
    app = SimulationContext("C5", factor=0.0)
    app.add_store("buffer", current_cap=20, max_cap=20, priority=True)
    got = []

    @app.task(name="stash", put={"store": "buffer", "priority": "attr.p"})
    def stash(task_id, entity):
        return {"n": entity.attributes["n"]}

    @app.task(name="drain", get={"store": "buffer"})
    def drain(task_id, item):
        got.append(item["n"])
        return {"taken": item["n"]}

    def script(context):
        for n, p in enumerate(priorities):
            context.spawn(stash(n, entity=Entity(f"e{n}", "x", {"p": p, "n": n})))
        yield context.env.timeout(1)
        for d in range(len(priorities)):
            context.spawn(drain(d))

    app.add_process(script)
    app.run(until=5)
    return got


def _yaml_order():
    """The items a YAML model put, in the order a get returns them."""
    _records, app = run_yaml("priority_store.yaml")
    store = app.get_store("buffer")
    assert store.priority
    heap = list(store.items)
    return [heapq.heappop(heap).item["kind"] for _ in range(len(heap))]


def test_c5_priority_item_storage():
    """Low first, then high: a get returns high, the lower priority value."""
    assert _reference_order() == ["high", "low"]
    assert _low_level_order() == ["high", "low"]
    assert _builder_order([5, 1]) == [1, 0]
    assert _yaml_order() == ["high", "low"]


# ------------------------------------------------ any orderable priority

TAKE_AT = 10


def _reference_any(priorities):
    """Plain SimPy: `PriorityItem((priority, sequence), n)`, the sequence breaking a
    tie in put order. Returns the order of n a run takes them in."""
    env = simpy.Environment()
    store = simpy.PriorityStore(env)
    out = []

    def flow():
        for n, priority in enumerate(priorities):
            yield store.put(simpy.PriorityItem((priority, n), n))
        for _ in priorities:
            out.append((yield store.get()).item)

    env.process(flow())
    env.run()
    return out


def _builder_any(priorities):
    app = SimulationContext("C5", factor=0.0)
    app.add_store("buffer", priority=True)
    got = []
    stashes = []

    for n, priority in enumerate(priorities):

        @app.task(
            name=f"stash{n}",
            put={
                "store": "buffer",
                "priority": lambda task_id, payload, value=priority: value,
            },
        )
        def stash(task_id, n=n):
            return {"n": n}

        stashes.append(stash)

    @app.task(name="take", get={"store": "buffer"})
    def take(task_id, item):
        got.append(item["n"])
        return {"taken": item["n"]}

    def script(context):
        for n, stash in enumerate(stashes):
            yield context.env.timeout(1)
            context.spawn(stash(n))
        yield context.env.timeout(TAKE_AT)
        for n in range(len(stashes)):
            context.spawn(take(n))

    app.add_process(script)
    outcome = run_builder(app, 30, raises=True)
    return got, outcome.error


def _yaml_any(tmp_path, priorities, literal=False):
    """YAML: a text priority is an expression, so it is read from the entity's
    attribute `p`; with `literal`, each task gives its list as the priority."""
    arrivals, tasks = "", ""
    for n, priority in enumerate(priorities):
        shown = (
            repr(priority)
            if isinstance(priority, str)
            else list(priority)
            if (isinstance(priority, tuple))
            else priority
        )
        if literal:
            arrivals += f"  a{n}: {{replay: {{path: a{n}.csv}}, spawn: stash{n}}}\n"
            tasks += (
                f"  stash{n}:\n    put: {{store: buffer, priority: {shown}}}\n"
                f"    payload: {{n: {n}}}\n"
            )
        else:
            arrivals += (
                f"  a{n}:\n    replay: {{path: a{n}.csv}}\n    spawn: stash\n"
                f"    attributes: {{p: {shown}, n: {n}}}\n"
            )
    if not literal:
        tasks = (
            "  stash:\n    put: {store: buffer, priority: attr.p}\n"
            "    payload: {}\n    attributes: [n]\n"
        )
    text = f"""
simulation: {{sim_id: C5, factor: 0}}
stores:
  buffer: {{priority: true}}
arrivals:
{arrivals}  takes: {{replay: {{path: takes.csv}}, spawn: take}}
tasks:
{tasks}  take:
    get: {{store: buffer}}
    item_field: item
    payload: {{}}
run: {{until: 30}}
"""
    files = {f"a{n}": [n + 1] for n in range(len(priorities))}
    files["takes"] = [float(TAKE_AT + len(priorities))] * len(priorities)
    outcome = run_yaml_text(tmp_path, text, files)
    got = [
        r["value"]["item"]["n"]
        for r in outcome.records
        if r["stream_type"] == "event"
        and r["task"] == "take"
        and isinstance(r["value"], dict)
        and "item" in r["value"]
    ]
    return got, outcome.error


def _folder(tmp_path, name):
    folder = tmp_path / name
    folder.mkdir()
    return folder


def test_c5_priority_item_storage_any_orderable_type(tmp_path):
    """A priority is kept as it is given, so strings and tuples order as they do in
    SimPy, and equal priorities keep their put order."""
    cases = {
        "strings": (["P2", "P0", "P3", "P1"], [1, 3, 0, 2]),
        "equal strings": (["B", "A", "B", "A"], [1, 3, 0, 2]),
        "tuples": ([(1, 2), (1, 1), (0, 9), (1, 1)], [2, 1, 3, 0]),
        "numbers": ([3, 1, 2, 1], [1, 3, 2, 0]),
    }
    for name, (priorities, order) in cases.items():
        assert _reference_any(priorities) == order, name
        assert _builder_any(priorities) == (order, None), name
        assert _yaml_any(_folder(tmp_path, name), priorities) == (order, None), name


def test_c5_priority_yaml_list_literal(tmp_path):
    """A YAML list is a priority that orders as a tuple does: `[1, 2]` after `[1, 1]`,
    equal lists in put order."""
    priorities = [(1, 2), (1, 1), (1, 1)]
    got, error = _yaml_any(tmp_path, priorities, literal=True)
    assert error is None
    assert got == _reference_any(priorities) == [1, 2, 0]


def test_c5_priority_mixed_types_raise_type_error(tmp_path):
    """An int and a string cannot be ordered: the second put raises `TypeError` in
    SimPy, the builder and YAML."""
    env = simpy.Environment()
    store = simpy.PriorityStore(env)

    def flow():
        yield store.put(simpy.PriorityItem((1, 0), 0))
        yield store.put(simpy.PriorityItem(("A", 1), 1))

    env.process(flow())
    try:
        env.run()
    except TypeError:
        pass
    else:
        raise AssertionError("SimPy ordered an int against a string")
    _got, error = _builder_any([1, "A"])
    assert isinstance(error, TypeError)
    _got, error = _yaml_any(tmp_path, [1, "A"])
    assert isinstance(error, TypeError)
