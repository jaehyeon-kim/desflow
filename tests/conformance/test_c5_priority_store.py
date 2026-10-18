"""C5 priority item storage: a store returns the lowest put priority first.

The low-level API already stores and orders `PriorityItem`s directly, which
`tests/unit/test_store.py` checks against SimPy. This covers the native put-side
priority surface the builder and YAML add: a task's `put` carries a priority, the
item is wrapped and ordered, and a `get` returns the lowest priority first,
matching a plain SimPy `PriorityStore`.
"""

from pathlib import Path

import simpy

from desflow import Entity
from desflow.core.context import SimulationContext


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


def _builder_order():
    app = SimulationContext("C5", factor=0.0)
    app.add_store("buffer", current_cap=10, max_cap=10, priority=True)
    got = []

    @app.task(name="stash", put={"store": "buffer", "priority": "attr.p"})
    def stash(task_id, entity):
        return {"kind": entity.attributes["kind"]}

    @app.task(name="drain", get={"store": "buffer"})
    def drain(task_id, item):
        got.append(item["kind"])
        return {"taken": item["kind"]}

    def script(context):
        context.spawn(stash(0, entity=Entity("p5", "x", {"p": 5, "kind": "low"})))
        context.spawn(stash(1, entity=Entity("p1", "x", {"p": 1, "kind": "high"})))
        yield context.env.timeout(1)
        context.spawn(drain(0))
        context.spawn(drain(1))

    app.add_process(script)
    app.run(until=5)
    return got


def _yaml_store_is_priority():
    path = Path(__file__).parent / "blueprints" / "priority_store.yaml"
    app = SimulationContext.from_yaml(path)
    app.run()
    return app.get_store("buffer").priority


def test_priority_order_matches_simpy_through_the_builder():
    assert _reference_order() == ["high", "low"]
    assert _builder_order() == ["high", "low"]


def test_yaml_exposes_a_priority_store():
    assert _yaml_store_is_priority()


def _builder_equal_priority_order():
    app = SimulationContext("C5eq", factor=0.0)
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
        for n in range(6):
            context.spawn(stash(n, entity=Entity(f"e{n}", "x", {"p": 3, "n": n})))
        yield context.env.timeout(1)
        for d in range(6):
            context.spawn(drain(d))

    app.add_process(script)
    app.run(until=5)
    return got


def test_equal_priority_keeps_put_order():
    # All six items share a priority, so the sequence tiebreak alone decides the
    # order out. PriorityItem compares only its priority, so equal keys never
    # raise; without the tiebreak heapq returns them out of put order.
    assert _builder_equal_priority_order() == [0, 1, 2, 3, 4, 5]
