"""Stores and containers: no capacity limit, custom types, queue notifications."""

import logging
import math

import numpy as np
import pytest
import simpy
from simpy.core import BoundClass
from simpy.resources.store import StoreGet
import yaml

from desflow import (
    CapacityConfig,
    ContainerBase,
    ContainerConfig,
    DynamicContainer,
    DynamicRealtimeEnvironment,
    DynamicStore,
    SimParameter,
    SimulationContext,
    StoreBase,
)
from desflow.blueprint import BlueprintError
from desflow.core.expressions import Expression
from desflow.resources.store import DynamicFilterStoreGet
from tests.conformance import custom_types as custom
from tests.conformance.harness import run_builder, run_low_level


def _env(**holders):
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(SimParameter(sim_id="Shop", **holders))
    return env


# ------------------------------------------------------------ capacity


def test_a_store_without_a_capacity_has_no_limit_and_can_be_lowered_and_raised():
    config = CapacityConfig()
    env = _env(stores={"shelf": config})
    shelf = DynamicStore(env, "Shop", "shelf")
    assert shelf.capacity == math.inf
    assert (config.current_cap, config.max_cap) == (math.inf, math.inf)
    env.registry.update("Shop.stores.shelf.current_cap", 2)
    env.run(until=env.now + 1)
    assert shelf.capacity == 2
    env.registry.update("Shop.stores.shelf.current_cap", math.inf)
    env.run(until=env.now + 1)
    assert shelf.capacity == math.inf
    env.teardown()


def test_infinity_on_an_int_store_path_is_accepted_live():
    env = _env(stores={"shelf": CapacityConfig(current_cap=3, max_cap=3)})
    shelf = DynamicStore(env, "Shop", "shelf")
    env.registry.update("Shop.stores.shelf.max_cap", math.inf)
    env.run(until=env.now + 1)
    env.registry.update("Shop.stores.shelf.current_cap", "inf")
    env.run(until=env.now + 1)
    assert shelf.capacity == math.inf
    assert env.registry.get("Shop.stores.shelf.max_cap").value == math.inf
    env.teardown()


def test_an_infinite_max_cap_with_a_finite_current_cap():
    env = _env(stores={"shelf": CapacityConfig(current_cap=3, max_cap=math.inf)})
    shelf = DynamicStore(env, "Shop", "shelf")
    assert shelf.capacity == 3
    env.registry.update("Shop.stores.shelf.current_cap", 500)
    env.run(until=env.now + 1)
    assert shelf.capacity == 500
    env.teardown()


@pytest.mark.parametrize("kind", ["stores", "containers"])
def test_a_max_cap_alone_gives_the_current_cap(kind):
    config = CapacityConfig(max_cap=4)
    env = _env(**{kind: {"x": config}})
    assert (config.current_cap, config.max_cap) == (4, 4)
    env.teardown()


def test_a_container_without_a_capacity_has_no_limit():
    env = _env(containers={"tank": ContainerConfig(level=3)})
    tank = DynamicContainer(env, "Shop", "tank")
    assert tank.capacity == math.inf
    env.registry.update("Shop.containers.tank.current_cap", 5)
    env.run(until=env.now + 1)
    assert tank.capacity == 5
    env.teardown()


def test_a_capacity_change_reaches_a_custom_type():
    env = _env(stores={"dock": CapacityConfig(current_cap=1, max_cap=4)})
    dock = DynamicStore(env, "Shop", "dock", type=custom.Dock)
    assert isinstance(dock._store, custom.Dock)
    results = []

    def fill():
        for n in range(3):
            yield dock.put(n)
            results.append((n, env.now))

    def grow():
        yield env.timeout(2)
        env.registry.update("Shop.stores.dock.current_cap", 3)

    env.process(fill())
    env.process(grow())
    env.run(until=5)
    assert results == [(0, 0), (1, 2), (2, 2)]
    env.teardown()


def test_the_registry_accepts_infinity_and_survives_an_overflow(registry, caplog):
    registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            resources={"bench": CapacityConfig(current_cap=2, max_cap=5)},
            variables={"count": 3},
        )
    )
    registry.update("Shop.resources.bench.max_cap", "inf")
    assert registry.get("Shop.resources.bench.max_cap").value == math.inf
    with caplog.at_level(logging.ERROR):
        registry.update("Shop.variables.count", math.inf)
    assert registry.get("Shop.variables.count").value == 3
    assert "Update ignored" in caplog.text


def test_the_telemetry_capacity_of_a_store_with_no_limit_is_null():
    app = SimulationContext("Shop", factor=0)
    app.add_store("shelf")
    app.add_container("tank")
    app.with_stats("basic", interval=1)
    records = []

    from desflow.connectors.egress.base import BaseEgress

    class Capture(BaseEgress):
        async def run(self, egress_queue):
            import asyncio
            import queue

            while True:
                try:
                    records.extend(egress_queue.get_nowait())
                except queue.Empty:
                    await asyncio.sleep(0.005)

    app.add_egress(Capture())
    app.run(until=2)
    capacity = [
        r["value"]
        for r in records
        if r.get("path_id", "").endswith(("stores.shelf.capacity", "tank.capacity"))
    ]
    assert capacity and all(value is None for value in capacity)


# ------------------------------------------------------------ YAML and builder


def _blueprint(tmp_path, stores):
    path = tmp_path / "shop.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "simulation": {"sim_id": "Shop", "factor": 0},
                "stores": stores,
                "run": {"until": 1},
            }
        ).replace("'.inf'", ".inf")
    )
    return path


def test_yaml_accepts_infinity_and_no_capacity_for_a_store(tmp_path):
    path = _blueprint(tmp_path, {"a": {}, "b": {"current_cap": 2, "max_cap": math.inf}})
    app = SimulationContext.from_yaml(path)
    app.run()
    assert app.get_store("a").capacity == math.inf
    assert app.get_store("b").capacity == 2


def test_the_builder_refuses_a_fractional_store_capacity():
    with pytest.raises(ValueError, match="whole number"):
        SimulationContext("Shop").add_store("a", current_cap=2.5)


def test_a_type_must_subclass_its_base_and_cannot_have_a_priority():
    app = SimulationContext("Shop")
    with pytest.raises(ValueError, match="StoreBase"):
        app.add_store("a", type=simpy.Store)
    with pytest.raises(ValueError, match="priority"):
        app.add_store("b", priority=True, type=custom.Dock)
    with pytest.raises(ValueError, match="ContainerBase"):
        app.add_container("c", type=simpy.Container)
    app.add_store("d", type=custom.Dock)


def test_a_filter_needs_a_filter_store_type():
    app = SimulationContext("Shop")
    app.add_store("dock", type=custom.Dock)

    @app.task(name="pick", get={"store": "dock", "filter": "item.v == 1"})
    def pick(task_id):
        return {}

    with pytest.raises(ValueError, match="FilterStore"):
        app.run(until=1)


# ------------------------------------------------------------ transfers


def test_wait_false_is_refused_in_a_wait_branch_and_for_item_field(tmp_path):
    from desflow.core.flow import Wait

    with pytest.raises(ValueError, match="wait: false"):
        Wait({"get": {"store": "s", "wait": False}})
    with pytest.raises(ValueError, match="wait: false"):
        Wait({"put": {"store": "s", "wait": False}})

    text = """
simulation: {sim_id: Shop, factor: 0}
stores: {s: {}}
tasks:
  t:
    get: {store: s, wait: false}
    item_field: got
    payload: {}
run: {until: 1}
"""
    path = tmp_path / "m.yaml"
    path.write_text(text)
    with pytest.raises(BlueprintError, match="item_field"):
        SimulationContext.from_yaml(path)


def test_wait_false_refusals_in_the_builder():
    app = SimulationContext("Shop")
    app.add_store("s")
    with pytest.raises(ValueError, match="declares item"):

        @app.task(name="t", get={"store": "s", "wait": False})
        def t(task_id, item):
            return {}

    with pytest.raises(ValueError, match="needs a wait"):

        @app.task(name="u", get={"store": "s", "wait": False}, patience=1)
        def u(task_id):
            return {}


def test_item_field_is_refused_without_a_store_get(tmp_path):
    text = """
simulation: {sim_id: Shop, factor: 0}
tasks:
  t:
    item_field: got
    payload: {}
run: {until: 1}
"""
    path = tmp_path / "m.yaml"
    path.write_text(text)
    with pytest.raises(BlueprintError, match="item_field"):
        SimulationContext.from_yaml(path)


def test_a_priority_store_get_gives_the_payload_unwrapped():
    app = SimulationContext("Shop", factor=0)
    app.add_store("p", priority=True)
    got = []

    @app.task(name="stash", put={"store": "p", "priority": 1})
    def stash(task_id):
        return {"n": 7}

    @app.task(name="take", get={"store": "p"})
    def take(task_id, item):
        got.append(item)
        return {}

    def script(context):
        context.spawn(stash(0))
        yield context.env.timeout(1)
        context.spawn(take(0))

    app.add_process(script)
    app.run(until=3)
    assert got == [{"n": 7}]


# ------------------------------------------------------------ notifications


def test_one_notification_per_do_with_or_without_super():
    class WithSuper(StoreBase):
        def _do_get(self, event):
            return super()._do_get(event)

        def _do_put(self, event):
            return super()._do_put(event)

    class Without(custom.LastInFirstOut, StoreBase):
        pass

    for cls in (StoreBase, WithSuper, custom.Dock, Without):
        env = DynamicRealtimeEnvironment(factor=0)
        store = cls(env)
        calls = []
        store._on_change = lambda: calls.append(1)
        store.put("a")
        store.put("b")
        store.get()
        assert len(calls) == 3, cls
        env.teardown()


def test_a_container_notifies_on_a_level_change_not_on_a_true_return():
    class Level(ContainerBase):
        def _do_get(self, event):
            super()._do_get(event)
            return True  # even when nothing was taken

    class Silent(ContainerBase):
        def _do_get(self, event):
            self._level -= event.amount
            event.succeed()  # changes the level and returns None

    env = DynamicRealtimeEnvironment(factor=0)
    tank = Level(env, init=1)
    calls = []
    tank._on_change = lambda: calls.append(tank.level)
    tank.get(5)  # not enough: no change
    assert calls == []
    tank.get(1)
    assert calls == [0]
    silent = Silent(env, init=3)
    seen = []
    silent._on_change = lambda: seen.append(silent.level)
    silent.get(2)
    assert seen == [1]
    env.teardown()


def test_a_plain_simpy_environment_works_with_the_bases():
    env = simpy.Environment()
    store = StoreBase(env, capacity=1)
    tank = ContainerBase(env, capacity=1)
    store.put("a")
    waiting = store.put("b")
    tank.put(1)
    over = tank.put(1)
    assert len(store.put_queue) == 1 and len(tank.put_queue) == 1
    waiting.cancel()
    over.cancel()
    assert not store.put_queue and not tank.put_queue


def test_watchers_added_with_queues_hear_a_queue_change_only():
    env = _env(stores={"shelf": CapacityConfig(current_cap=1, max_cap=1)})
    shelf = DynamicStore(env, "Shop", "shelf")
    plain, queued = [], []

    def a():
        plain.append(1)

    def b():
        queued.append(1)

    env.watch_changes(a)
    env.watch_changes(b, queues=True)
    env.notify_queue_change()
    assert (len(plain), len(queued)) == (0, 1)
    env.notify_change()
    assert (len(plain), len(queued)) == (1, 2)
    shelf.put("x")
    before = (len(plain), len(queued))
    waiting = shelf.put("y")  # joins the put queue: a queue change
    assert len(queued) == before[1] + 1 and len(plain) == before[0]
    waiting.cancel()  # leaves it: another
    assert len(queued) == before[1] + 2
    env.unwatch_changes(b)
    env.notify_queue_change()
    assert len(queued) == before[1] + 2
    env.unwatch_changes(a)
    env.teardown()


def test_a_queue_change_leaves_the_level_average_alone():
    app = SimulationContext("Shop", factor=0)
    app.add_store("shelf", current_cap=1, max_cap=1)
    app.with_stats("basic", interval=1000)

    def model(context):
        shelf = context.get_store("shelf")
        yield context.env.timeout(2)
        shelf.put("a")
        yield context.env.timeout(2)
        waiting = shelf.put("b")  # a queue change, not a level change
        yield context.env.timeout(2)
        waiting.cancel()

    app.add_process(model)
    app.run(until=6)
    readings = app.env.stats.readings()
    assert readings["Shop.stats.stores.shelf.level_avg"] == pytest.approx(4 / 6)


@pytest.mark.parametrize(
    "text,expected",
    [
        ("level.stock >= 1", False),
        ("putters.shelf >= 1", True),
        ("getters.shelf == 0", True),
        ("count(shelf) >= 1", True),
        ("queue.packers > 1", True),
        ("t > 3", False),
    ],
)
def test_an_expression_reads_queues_only_when_it_names_one(text, expected):
    assert Expression(text).reads_queues is expected


def test_two_gets_granted_in_one_trigger_notify_between_the_grants():
    """A put of 2 grants two waiting gets of 1 in one trigger. SimPy's levels after
    each grant are the order a `level` condition must see them in."""
    reference = []

    class Logged(simpy.Container):
        def _do_get(self, event):
            before = self._level
            result = super()._do_get(event)
            if self._level != before:
                reference.append(self._level)
            return result

    senv = simpy.Environment()
    tank = Logged(senv, capacity=10)
    tank.get(1)
    tank.get(1)
    tank.put(2)
    senv.run()

    env = _env(containers={"tank": ContainerConfig(current_cap=10, max_cap=10)})
    stock = DynamicContainer(env, "Shop", "tank")
    stock.get(1)
    stock.get(1)
    seen = []
    env.watch_changes(lambda: seen.append(stock.level))
    stock.put(2)
    env.run(until=1)
    assert reference == [1, 0]
    assert seen == [2, 1, 0]  # the put, then one notification per grant
    env.teardown()


def test_negative_infinity_is_not_a_store_capacity(tmp_path):
    with pytest.raises(ValueError, match="whole number"):
        SimulationContext("Shop").add_store("a", current_cap=float("-inf"))
    path = _blueprint(tmp_path, {"a": {"current_cap": -math.inf}})
    with pytest.raises(BlueprintError, match="-inf"):
        SimulationContext.from_yaml(path)


def _waiting(kind, side):
    """The times a watcher on the queues sees a put or a get waiting, when the
    only one made is granted at once."""
    params = SimParameter(
        sim_id="C5",
        stores={"s": CapacityConfig(current_cap=10, max_cap=10)},
        containers={"c": ContainerConfig(current_cap=10, max_cap=10, level=5)},
    )
    seen = []

    def start(env):
        if kind == "store":
            holder = DynamicStore(env, "C5", "s", items=["a", "b", "c"])
        else:
            holder = DynamicContainer(env, "C5", "c")
        read = (
            (lambda: holder.putters_waiting)
            if side == "put"
            else (lambda: holder.getters_waiting)
        )

        def check():
            if read() >= 1:
                seen.append(env.now)

        env.watch_changes(check, queues=True)

        def one():
            yield env.timeout(1)
            if side == "put":
                yield holder.put("x") if kind == "store" else holder.put(1)
            else:
                yield holder.get() if kind == "store" else holder.get(1)

        env.process(one())

    run_low_level(params, start, 3)
    return seen


@pytest.mark.parametrize("kind", ["store", "container"])
@pytest.mark.parametrize("side", ["put", "get"])
def test_a_request_granted_at_once_is_not_waiting(kind, side):
    assert _waiting(kind, side) == []


def test_a_condition_on_putters_ignores_a_put_granted_at_once():
    app = SimulationContext("C5", factor=0.0)
    app.add_store("shelf")  # no limit: every put is granted at once
    app.add_arrival("puts", dist="constant", value=1)

    @app.task(name="put", put={"store": "shelf"})
    def put(task_id):
        return {"event": "put"}

    @app.task(name="watcher", wait={"condition": "putters.shelf >= 1"})
    def watcher(task_id):
        return {"event": "resumed"}

    def loop(context):
        while True:
            entity = yield context.wait_for_arrival("puts")
            context.spawn(put(context.next_instance("put"), entity=entity))

    app.arrival_loop("puts")(loop)

    def later(context):
        yield context.env.timeout(0.5)
        context.spawn(watcher(0))

    app.add_process(later)
    records = run_builder(app, 5)
    resumed = [r for r in records if r.get("value", {}).get("event") == "resumed"]
    assert resumed == []


def test_a_fractional_live_capacity_is_rounded_down_in_the_registry_and_the_store():
    params = SimParameter(sim_id="X", stores={"s": CapacityConfig()})
    seen = {}

    def start(env):
        store = DynamicStore(env, "X", "s")

        def change():
            yield env.timeout(1)
            env.registry.update("X.stores.s.current_cap", 2.5)
            yield env.timeout(1)
            seen["store"] = store.capacity
            seen["registry"] = env.registry.get("X.stores.s.current_cap").value

        env.process(change())

    run_low_level(params, start, 3)
    assert seen == {"store": 2, "registry": 2}


class _FilterLane(StoreBase, simpy.FilterStore):
    pass


def test_a_store_type_that_is_also_a_filter_store_takes_a_filtered_get():
    app = SimulationContext("F", factor=0.0)
    app.add_store("s", items=[{"k": 1}, {"k": 2}], type=_FilterLane)
    app.add_arrival("a", dist="constant", value=1)

    @app.task(name="take", get={"store": "s", "filter": "item.k == 2"})
    def take(task_id, item):
        return {"got": item}

    def loop(context):
        entity = yield context.wait_for_arrival("a")
        context.spawn(take(context.next_instance("take"), entity=entity))

    app.arrival_loop("a")(loop)
    records = run_builder(app, 3)
    got = [r["value"] for r in records if r.get("value", {}).get("got")]
    assert got and got[0]["got"] == {"k": 2}


class _MyGet(DynamicFilterStoreGet):
    pass


class _OwnGet(StoreBase, simpy.FilterStore):
    get = BoundClass(_MyGet)


class _OwnGetChild(_OwnGet):
    pass


class _GetMixin:
    get = BoundClass(_MyGet)


class _MixinFirst(_GetMixin, StoreBase, simpy.FilterStore):
    pass


@pytest.mark.parametrize("cls", [_OwnGet, _OwnGetChild, _MixinFirst])
def test_a_get_event_class_of_your_own_is_kept(cls):
    assert cls.get is _MyGet


@pytest.mark.parametrize(
    "cls", [_FilterLane, type("B", (simpy.FilterStore, StoreBase), {})]
)
def test_a_default_get_becomes_the_filtered_one(cls):
    assert cls.get is DynamicFilterStoreGet


def test_a_capacity_given_as_text_is_refused_and_a_numpy_integer_is_not():
    for value in ("3", "ten", b"3"):
        with pytest.raises(ValueError, match="whole number"):
            SimulationContext("Shop").add_store("a", current_cap=value)
    SimulationContext("Shop").add_store("a", current_cap=np.int64(3))
    SimulationContext("Shop").add_store("b", current_cap=True)


class _PlainGetFilterStore(StoreBase, simpy.FilterStore):
    get = BoundClass(StoreGet)


class _PlainGetChild(_PlainGetFilterStore):
    pass


@pytest.mark.parametrize("cls", [_PlainGetFilterStore, _PlainGetChild])
def test_a_filter_store_type_that_sets_a_default_get_keeps_it(cls):
    assert cls.get is StoreGet
