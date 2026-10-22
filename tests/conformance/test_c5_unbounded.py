"""C5 stores and containers with no capacity limit: a put never waits.

A store with no `current_cap` and a container whose capacity is infinity run the
bounded-store jobs of `test_c5_bounded.py` with puts every 1 s and gets every 5 s.
A plain SimPy `Store` or `Container` of infinite capacity sets the times every API
must match.
"""

import math

from desflow import (
    CapacityConfig,
    ContainerConfig,
    DynamicContainer,
    DynamicStore,
    SimParameter,
)
from desflow.core.context import SimulationContext
from tests.conformance import test_c5_bounded as bounded
from tests.conformance.harness import run_builder, run_low_level, run_yaml

UNTIL = bounded.UNTIL


def _params(kind):
    if kind == "store":
        return SimParameter(sim_id="C5", stores={"bin": CapacityConfig()})
    return SimParameter(
        sim_id="C5",
        containers={"bin": ContainerConfig(current_cap=math.inf, level=0)},
    )


def _low_level(kind):
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
                    event = holder.put(f"item-{n}" if kind == "store" else 2)
                else:
                    event = holder.get() if kind == "store" else holder.get(3)
                env.process(task(name, n, event))

        env.process(source(1, "put"))
        env.process(source(5, "get"))

    records, _env = run_low_level(_params(kind), start, UNTIL)
    return records


def _builder(kind):
    app = SimulationContext("C5", factor=0.0)
    if kind == "store":
        app.add_store("bin")
        put_spec, get_spec = {"store": "bin"}, {"store": "bin"}
    else:
        app.add_container("bin", current_cap=math.inf)
        put_spec = {"container": "bin", "amount": 2}
        get_spec = {"container": "bin", "amount": 3}
    app.add_arrival("puts", dist="constant", value=1)
    app.add_arrival("gets", dist="constant", value=5)

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


def _check(kind, blueprint):
    expected = bounded._reference(kind, 1, 5, math.inf, 2, 3)
    yaml_records, app = run_yaml(blueprint)
    for records in (_low_level(kind), _builder(kind), yaml_records):
        assert bounded._finished(records) == expected
    # No put waited: each finished at the moment it arrived
    assert expected["put"] == list(range(1, UNTIL))
    return app


def test_c5_store_with_no_capacity_limit():
    """A store with no `current_cap` is unbounded: every put at 1, 2, 3 ... finishes
    as it arrives, and the gets at 5, 10, 15, 20 take the oldest items."""
    app = _check("store", "store_unbounded.yaml")
    store = app.get_store("bin")
    assert store.capacity == math.inf
    assert len(store.items) == UNTIL - 1 - 4


def test_c5_container_with_no_capacity_limit():
    """A container with `current_cap: .inf` never makes a put wait."""
    app = _check("container", "container_unbounded.yaml")
    container = app.get_container("bin")
    assert container.capacity == math.inf
    assert container.level == 2 * (UNTIL - 1) - 3 * 4
