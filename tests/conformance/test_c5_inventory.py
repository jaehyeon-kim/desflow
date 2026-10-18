"""C5 filtered retrieval and cancelled puts and gets.

Filtered retrieval: two orders wait for a parcel of their own size, a large one
from 10 and a small one from 11. A small parcel put at 12 goes to the second order,
because the first does not match, and a large one at 13 to the first, as in SimPy's
`FilterStore`.

Cancellation: a get that gives up before it is served takes nothing later, and a
put that gives up adds nothing; the store's items and the container's level are
unchanged by it, as SimPy's `Get.cancel()` and `Put.cancel()` promise. The builder
and YAML cancel a get through `patience`, and a put through an interrupt with
`on_interrupt: abandon`.
"""

import simpy

from desflow import (
    CapacityConfig,
    ContainerConfig,
    DynamicContainer,
    DynamicStore,
    Entity,
    SimParameter,
)
from desflow.core.context import SimulationContext
from tests.conformance.harness import run_builder, run_low_level, run_yaml

ORDERS = {"large": (10, "L"), "small": (11, "S")}  # name: (arrives at, size wanted)
PARCELS = [(12, "S"), (13, "L")]  # put at, size
EXPECTED_FILTERED = [(12.0, "small", "S"), (13.0, "large", "L")]


def _reference_filtered():
    env = simpy.Environment()
    store = simpy.FilterStore(env)
    served = []

    def order(name, size):
        parcel = yield store.get(lambda item: item["size"] == size)
        served.append((env.now, name, parcel["size"]))

    def at(time, make):
        yield env.timeout(time)
        make()

    for name, (time, size) in ORDERS.items():
        env.process(at(time, lambda n=name, s=size: env.process(order(n, s))))
    for time, size in PARCELS:
        env.process(at(time, lambda s=size: store.put({"size": s})))
    env.run(until=14)
    return served


def _served(records):
    return [
        (r["sim_ts"], r["key"].split("-")[0], r["value"]["parcel"]["size"])
        for r in records
        if r["stream_type"] == "event" and "parcel" in r["value"]
    ]


def _low_level_filtered():
    def start(env):
        store = DynamicStore(env, "C5f", "parcels")

        def order(name, size):
            parcel = yield store.get(filter=lambda item: item["size"] == size)
            env.publish_event(f"{name}-1", {"parcel": parcel}, task="take", instance=0)

        def at(time, make):
            yield env.timeout(time)
            make()

        for name, (time, size) in ORDERS.items():
            env.process(at(time, lambda n=name, s=size: env.process(order(n, s))))
        for time, size in PARCELS:
            env.process(at(time, lambda s=size: store.put({"size": s})))

    params = SimParameter(
        sim_id="C5f", stores={"parcels": CapacityConfig(current_cap=5, max_cap=5)}
    )
    return run_low_level(params, start, 14)[0]


def _builder_filtered():
    app = SimulationContext("C5f", factor=0.0)
    app.add_store("parcels", current_cap=5, max_cap=5)

    @app.task(name="take", get={"store": "parcels", "filter": "item.size == attr.size"})
    def take(task_id, item):
        return {"parcel": item}

    @app.task(name="drop", put={"store": "parcels"})
    def drop(task_id, entity):
        return {"size": entity.attributes["size"]}

    def script(context):
        events = sorted(
            [(t, "take", n, s) for n, (t, s) in ORDERS.items()]
            + [(t, "drop", f"parcel{t}", s) for t, s in PARCELS]
        )
        for time, task, name, size in events:
            if time > context.env.now:
                yield context.env.timeout(time - context.env.now)
            body = take if task == "take" else drop
            entity = Entity(f"{name}-1", name, {"size": size})
            context.spawn(body(context.next_instance(task), entity=entity))

    app.add_process(script)
    return run_builder(app, 14)


def test_c5_filtered_retrieval():
    assert _reference_filtered() == EXPECTED_FILTERED
    for records in (
        _low_level_filtered(),
        _builder_filtered(),
        run_yaml("filtered_store.yaml")[0],
    ):
        assert _served(records) == EXPECTED_FILTERED


# Cancellation: a getter waits from 1 with a patience of 2 and gives up at 3; an
# item or an amount is put at 5. It stays where it was put.


def _reference_cancel(kind, direction):
    """What the holder holds at 6, after a get or put that gave up at 3."""
    env = simpy.Environment()
    if kind == "store":
        holder = simpy.Store(env, capacity=1)
    else:
        holder = simpy.Container(env, capacity=4, init=0)

    def gives_up():
        yield env.timeout(1)
        if direction == "get":
            event = holder.get() if kind == "store" else holder.get(3)
        else:
            event = holder.put("late") if kind == "store" else holder.put(3)
        yield event | env.timeout(2)
        assert not event.triggered
        event.cancel()

    def fill():
        # A put cancellation needs a full holder first; a get one needs it empty
        if direction == "put":
            yield holder.put("first") if kind == "store" else holder.put(2)
        yield env.timeout(5)
        if direction == "get":
            yield holder.put("parcel") if kind == "store" else holder.put(2)
        else:
            yield holder.get() if kind == "store" else holder.get(2)

    env.process(gives_up())
    env.process(fill())
    env.run(until=6)
    return list(holder.items) if kind == "store" else holder.level


def _low_level_cancel(kind, direction):
    built = {}

    def start(env):
        holder = (
            DynamicStore(env, "C5c", "bin")
            if kind == "store"
            else DynamicContainer(env, "C5c", "bin")
        )
        built["holder"] = holder

        def gives_up():
            yield env.timeout(1)
            if direction == "get":
                event = holder.get() if kind == "store" else holder.get(3)
            else:
                event = holder.put("late") if kind == "store" else holder.put(3)
            yield event | env.timeout(2)
            event.cancel()

        def fill():
            if direction == "put":
                yield holder.put("first") if kind == "store" else holder.put(2)
            yield env.timeout(5)
            if direction == "get":
                yield holder.put("parcel") if kind == "store" else holder.put(2)
            else:
                yield holder.get() if kind == "store" else holder.get(2)

        env.process(gives_up())
        env.process(fill())

    if kind == "store":
        params = SimParameter(
            sim_id="C5c", stores={"bin": CapacityConfig(current_cap=1, max_cap=1)}
        )
    else:
        params = SimParameter(
            sim_id="C5c",
            containers={"bin": ContainerConfig(current_cap=4, max_cap=4, level=0)},
        )
    run_low_level(params, start, 6)
    holder = built["holder"]
    return list(holder.items) if kind == "store" else holder.level


def _builder_cancel_get(kind):
    app = SimulationContext("C5c", factor=0.0)
    if kind == "store":
        app.add_store("bin", current_cap=1, max_cap=1)
        spec, give = {"store": "bin"}, {"store": "bin"}
    else:
        app.add_container("bin", current_cap=4, max_cap=4)
        spec = {"container": "bin", "amount": 3}
        give = {"container": "bin", "amount": 2}

    @app.task(name="take", get=spec, patience=2)
    def take(task_id):
        return {"event": "taken"}

    @app.task(name="fill", put=give)
    def fill(task_id):
        return {"event": "filled"}

    def script(context):
        yield context.env.timeout(1)
        context.spawn(take(0))
        yield context.env.timeout(4)
        context.spawn(fill(0))

    app.add_process(script)
    records = run_builder(app, 6)
    holder = app.get_store("bin") if kind == "store" else app.get_container("bin")
    return records, (list(holder.items) if kind == "store" else holder.level)


def _yaml_cancel_get(kind):
    records, app = run_yaml(f"cancel_{kind}_get.yaml")
    holder = app.get_store("bin") if kind == "store" else app.get_container("bin")
    return records, (list(holder.items) if kind == "store" else holder.level)


def _reneged_at(records):
    return [
        r["sim_ts"]
        for r in records
        if r["stream_type"] == "event" and r["value"].get("status") == "reneged"
    ]


def test_c5_cancel_store_get():
    """The get gives up at 3, so the parcel put at 5 stays in the store."""
    assert _reference_cancel("store", "get") == ["parcel"]
    assert _low_level_cancel("store", "get") == ["parcel"]
    # YAML arrivals start at their first gap, so its get gives up at 12
    for (records, items), reneged in (
        (_builder_cancel_get("store"), 3.0),
        (_yaml_cancel_get("store"), 12.0),
    ):
        assert _reneged_at(records) == [reneged]
        assert items == [{"event": "filled"}]  # the payload of the put


def test_c5_cancel_quantity_get():
    """The get of 3 gives up at 3, so the 2 put at 5 stay in the container."""
    assert _reference_cancel("container", "get") == 2
    assert _low_level_cancel("container", "get") == 2
    for (records, level), reneged in (
        (_builder_cancel_get("container"), 3.0),
        (_yaml_cancel_get("container"), 12.0),
    ):
        assert _reneged_at(records) == [reneged]
        assert level == 2


def _builder_cancel_put(kind):
    """A put that waits from 1 for room, interrupted at 3 with abandon; a get at 5
    makes room the cancelled put never takes."""
    app = SimulationContext("C5c", factor=0.0)
    if kind == "store":
        app.add_store("bin", current_cap=1, max_cap=1, items=["first"])
        give, take = {"store": "bin"}, {"store": "bin"}
    else:
        app.add_container("bin", current_cap=4, max_cap=4, level=2)
        give = {"container": "bin", "amount": 3}
        take = {"container": "bin", "amount": 2}

    @app.task(name="fill", put=give, on_interrupt="abandon")
    def fill(task_id):
        return {"event": "late"}

    @app.task(name="take", get=take)
    def take_task(task_id):
        return {"event": "taken"}

    def script(context):
        yield context.env.timeout(1)
        context.spawn(fill(0))
        yield context.env.timeout(2)
        context.interrupt("fill", cause="recall")
        yield context.env.timeout(2)
        context.spawn(take_task(0))

    app.add_process(script)
    run_builder(app, 6)
    holder = app.get_store("bin") if kind == "store" else app.get_container("bin")
    return list(holder.items) if kind == "store" else holder.level


def _yaml_cancel_put(kind):
    _records, app = run_yaml(f"cancel_{kind}_put.yaml")
    holder = app.get_store("bin") if kind == "store" else app.get_container("bin")
    return list(holder.items) if kind == "store" else holder.level


def test_c5_cancel_store_put():
    """The put into a full store gives up at 3; a get at 5 takes the first item,
    and the late one never goes in."""
    assert _reference_cancel("store", "put") == []
    assert _low_level_cancel("store", "put") == []
    assert _builder_cancel_put("store") == []
    assert _yaml_cancel_put("store") == []


def test_c5_cancel_quantity_put():
    """The put of 3 into a container holding 2 of 4 gives up at 3; after a get of
    2 at 5 the level is 0, not 3."""
    assert _reference_cancel("container", "put") == 0
    assert _low_level_cancel("container", "put") == 0
    assert _builder_cancel_put("container") == 0
    assert _yaml_cancel_put("container") == 0
