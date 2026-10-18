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
from tests.conformance.harness import run_low_level, run_yaml


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


def test_c5_priority_item_storage_equal_priorities():
    """Six items of one priority come out in put order. A `PriorityItem` compares
    only its priority, so without the put order heapq returns them mixed."""
    assert _builder_order([3] * 6) == [0, 1, 2, 3, 4, 5]
