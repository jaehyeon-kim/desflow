"""C5 bounded stores and containers: a full put waits for room, an empty get waits
for an item or an amount.

A producer arrives every `put_gap` seconds and adds one item, or `amount` to a
container; a consumer arrives every `get_gap` seconds and takes one. Both wait as
long as they must. A plain SimPy `Store` or `Container` with the same arrivals sets
the time each put and get completes, which every API must match. When the producer
is faster the puts block (the put checks); when the consumer is faster the gets block
(the get checks).
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
from tests.conformance.harness import event_trace, run_builder, run_low_level, run_yaml

UNTIL = 21


def _reference(kind, put_gap, get_gap, capacity, put_amount, get_amount):
    """Completion times of every put and get, by task name, in plain SimPy."""
    env = simpy.Environment()
    if kind == "store":
        holder = simpy.Store(env, capacity=capacity)
    else:
        holder = simpy.Container(env, capacity=capacity, init=0)
    done = {"put": [], "get": []}

    def put(n):
        yield holder.put(f"item-{n}") if kind == "store" else holder.put(put_amount)
        done["put"].append(env.now)

    def get():
        yield holder.get() if kind == "store" else holder.get(get_amount)
        done["get"].append(env.now)

    def source(gap, make):
        n = 0
        while True:
            yield env.timeout(gap)
            n += 1
            env.process(make(n))

    env.process(source(put_gap, put))
    env.process(source(get_gap, lambda n: get()))
    env.run(until=UNTIL)
    return done


def _finished(records):
    """Completion times of every put and get task, from its finished event."""
    done = {"put": [], "get": []}
    for t, _key, task, status in event_trace(records):
        if status is None:  # a finished event's payload has no status
            done[task].append(t)
    return done


def _params(kind, capacity):
    if kind == "store":
        return SimParameter(
            sim_id="C5",
            stores={"bin": CapacityConfig(current_cap=capacity, max_cap=capacity)},
        )
    return SimParameter(
        sim_id="C5",
        containers={
            "bin": ContainerConfig(current_cap=capacity, max_cap=capacity, level=0)
        },
    )


def _low_level(kind, put_gap, get_gap, capacity, put_amount, get_amount):
    def start(env):
        holder = (
            DynamicStore(env, "C5", "bin")
            if kind == "store"
            else DynamicContainer(env, "C5", "bin")
        )

        def task(name, n, event):
            key = f"{name}s-{n}"
            env.publish_event(key, {"status": "queued"}, task=name, instance=n)
            yield event
            env.publish_event(key, {"status": "started"}, task=name, instance=n)
            env.publish_event(key, {"event": name}, task=name, instance=n)

        def source(gap, name):
            n = 0
            while True:
                yield env.timeout(gap)
                n += 1
                if name == "put":
                    event = (
                        holder.put(f"item-{n}")
                        if kind == "store"
                        else holder.put(put_amount)
                    )
                else:
                    event = holder.get() if kind == "store" else holder.get(get_amount)
                env.process(task(name, n, event))

        env.process(source(put_gap, "put"))
        env.process(source(get_gap, "get"))

    records, _env = run_low_level(_params(kind, capacity), start, UNTIL)
    return records


def _builder(kind, put_gap, get_gap, capacity, put_amount, get_amount):
    app = SimulationContext("C5", factor=0.0)
    if kind == "store":
        app.add_store("bin", current_cap=capacity, max_cap=capacity)
        put_spec, get_spec = {"store": "bin"}, {"store": "bin"}
    else:
        app.add_container("bin", current_cap=capacity, max_cap=capacity)
        put_spec = {"container": "bin", "amount": put_amount}
        get_spec = {"container": "bin", "amount": get_amount}
    app.add_arrival("puts", dist="constant", value=put_gap)
    app.add_arrival("gets", dist="constant", value=get_gap)

    @app.task(name="put", put=put_spec)
    def put(task_id):
        return {"event": "put"}

    @app.task(name="get", get=get_spec)
    def get(task_id):
        return {"event": "get"}

    for arrival, task, name in (("puts", put, "put"), ("gets", get, "get")):

        def loop(context, arrival=arrival, task=task, name=name):
            while True:
                entity = yield context.wait_for_arrival(arrival)
                context.spawn(task(context.next_instance(name), entity=entity))

        app.arrival_loop(arrival)(loop)
    return run_builder(app, UNTIL)


def _check(kind, put_gap, get_gap, capacity, put_amount, get_amount, blueprint):
    args = (kind, put_gap, get_gap, capacity, put_amount, get_amount)
    expected = _reference(*args)
    for records in (
        _low_level(*args),
        _builder(*args),
        run_yaml(blueprint)[0],
    ):
        assert _finished(records) == expected
    return expected


def test_c5_bounded_store_put():
    """Puts every 2 s into a store of 1, gets every 5 s: the put of 4 waits for
    the get of 5."""
    done = _check("store", 2, 5, 1, 1, 1, "store_put.yaml")
    assert done["put"][:3] == [2, 5, 10]


def test_c5_bounded_store_get():
    """Puts every 5 s into a store of 1, gets every 2 s: the get of 2 waits for the
    put of 5."""
    done = _check("store", 5, 2, 1, 1, 1, "store_get.yaml")
    assert done["get"][:2] == [5, 10]


def test_c5_bounded_quantity_put():
    """Adds 2 every 2 s to a container of 4, takes 3 every 5 s: the put of 8 waits
    for room until the get of 10."""
    done = _check("container", 2, 5, 4, 2, 3, "container_put.yaml")
    assert done["put"][:5] == [2, 4, 6, 10, 10]


def test_c5_bounded_quantity_get():
    """Adds 2 every 5 s to a container of 4, takes 3 every 2 s: the get of 2 waits
    until the level covers it at 10."""
    done = _check("container", 5, 2, 4, 2, 3, "container_get.yaml")
    assert done["get"] == [10, 15]
