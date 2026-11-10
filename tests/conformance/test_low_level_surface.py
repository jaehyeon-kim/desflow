"""The SimPy attributes a Low-level program reads: gaps L1 and L2.

Each test runs a SimPy 4.1.2 program and the same program on desflow's Low-level
resources, and compares what the program reads. L1: `put_queue`, `get_queue`,
`PutQueue` and `GetQueue` on a store and a container, `put` and `get` on a
resource, and `isinstance` against SimPy's classes. L2: the `items` assignment of
the `FilterStore` example in the SimPy resources guide.

The waiting counts are 1, 3 and 10, because a queue of one cannot show an order.
"""

import pytest
import simpy
from simpy.resources.container import Container, ContainerGet, ContainerPut
from simpy.resources.resource import PriorityRequest, Request, Resource
from simpy.resources.store import FilterStore, Store, StoreGet, StorePut

from desflow import (
    CapacityConfig,
    ContainerConfig,
    DynamicContainer,
    DynamicResource,
    DynamicStore,
    SimParameter,
)
from tests.conformance.harness import run_low_level

WAITING = [1, 3, 10]


def _params(**sections):
    return SimParameter(sim_id="L", **sections)


def _simpy_queues(kind, waiting):
    """What a SimPy program reads from a full holder with `waiting` puts and
    `waiting` gets queued: the queue types, their lengths and the order of the
    queued events."""
    env = simpy.Environment()
    if kind == "store":
        holder = Store(env, capacity=1)
        holder.put("a")
        puts = [holder.put(f"p{n}") for n in range(waiting)]
        gets = []
    else:
        holder = Container(env, capacity=1, init=1)
        puts = [holder.put(1) for _ in range(waiting)]
        gets = []
    env.run()
    # Gets queue only when nothing can be taken: drain with a second holder
    empty = Store(env, capacity=1) if kind == "store" else Container(env, 1, init=0)
    gets = [empty.get() if kind == "store" else empty.get(1) for _ in range(waiting)]
    env.run()
    return {
        "put_type": type(holder.put_queue).__name__,
        "get_type": type(empty.get_queue).__name__,
        "put_len": len(holder.put_queue),
        "get_len": len(empty.get_queue),
        "put_in_order": holder.put_queue == puts,
        "get_in_order": empty.get_queue == gets,
        "PutQueue": holder.PutQueue is list,
        "GetQueue": empty.GetQueue is list,
    }


def _desflow_queues(kind, waiting):
    """The same reads on desflow's Low-level store or container."""
    seen = {}

    def start(env):
        make = DynamicStore if kind == "store" else DynamicContainer
        holder = make(env, "L", "full")
        empty = make(env, "L", "empty")
        if kind == "store":
            holder.put("a")
            puts = [holder.put(f"p{n}") for n in range(waiting)]
            gets = [empty.get() for _ in range(waiting)]
        else:
            puts = [holder.put(1) for _ in range(waiting)]
            gets = [empty.get(1) for _ in range(waiting)]
        seen.update(
            put_type=type(holder.put_queue).__name__,
            get_type=type(empty.get_queue).__name__,
            put_len=len(holder.put_queue),
            get_len=len(empty.get_queue),
            put_in_order=holder.put_queue == puts,
            get_in_order=empty.get_queue == gets,
            PutQueue=holder.PutQueue is list,
            GetQueue=empty.GetQueue is list,
        )
        yield env.timeout(1)

    if kind == "store":
        params = _params(
            stores={
                "full": CapacityConfig(current_cap=1, max_cap=1),
                "empty": CapacityConfig(current_cap=1, max_cap=1),
            }
        )
    else:
        params = _params(
            containers={
                "full": ContainerConfig(current_cap=1, max_cap=1, level=1),
                "empty": ContainerConfig(current_cap=1, max_cap=1, level=0),
            }
        )
    run_low_level(params, lambda env: env.process(start(env)), until=2)
    return seen


@pytest.mark.parametrize("waiting", WAITING)
@pytest.mark.parametrize("kind", ["store", "container"])
def test_queue_attributes_exist(kind, waiting):
    """A store and a container have `put_queue`, `get_queue`, `PutQueue` and
    `GetQueue`, and they read as SimPy's: lists of the waiting events in the order
    they queued, which happened (the SimPy run holds `waiting` of each)."""
    reference = _simpy_queues(kind, waiting)

    assert reference["put_len"] == reference["get_len"] == waiting
    assert reference["put_in_order"] and reference["get_in_order"]
    assert _desflow_queues(kind, waiting) == reference


@pytest.mark.parametrize("waiting", WAITING)
def test_resource_put_and_get_exist_and_put_takes_a_unit(waiting):
    """SimPy's `Resource` has `put` and `get` (the base class's, which `request` and
    `release` specialise). A unit taken with `put()` holds the resource and the
    next `put()` queues, in order, as `request()` does. SimPy's `get()` takes no
    argument and cannot release a unit (its `Get` has no request), so a program
    gives a unit back with `release(request)`; desflow's `get(request)` is that
    release."""
    env = simpy.Environment()
    resource = Resource(env, capacity=1)
    assert callable(resource.put) and callable(resource.get)
    first = resource.put()
    queued = [resource.put() for _ in range(waiting)]
    env.run()
    reference = [first.triggered] + [q.triggered for q in queued]
    resource.release(first)
    env.run()
    reference += [q.triggered for q in queued]
    assert reference[: 1 + waiting] == [True] + [False] * waiting
    assert reference[1 + waiting :] == [True] + [False] * (waiting - 1)

    seen = []

    def start(env):
        dynamic = DynamicResource(env, "L", "r")
        first = dynamic.put()
        queued = [dynamic.put() for _ in range(waiting)]
        yield env.timeout(1)
        seen.extend([first.triggered] + [q.triggered for q in queued])
        dynamic.get(first)
        yield env.timeout(1)
        seen.extend(q.triggered for q in queued)

    params = _params(resources={"r": CapacityConfig(current_cap=1, max_cap=1)})
    run_low_level(params, lambda env: env.process(start(env)), until=3)

    assert seen == reference


def test_isinstance_of_simpy_classes():
    """A Low-level store, container and request are instances of SimPy's classes,
    as SimPy's own are, so code that checks the type finds what it expects. The
    put and get events a store hands out are SimPy's `StorePut` and `StoreGet`, as
    they are in SimPy."""
    env = simpy.Environment()
    simpy_store, simpy_container = FilterStore(env), Container(env)
    simpy_request = Resource(env).request()
    assert isinstance(simpy_store, Store) and isinstance(simpy_container, Container)
    assert isinstance(simpy_request, Request)
    assert isinstance(simpy_store.put("x"), StorePut)
    assert isinstance(simpy_store.get(), StoreGet)
    assert isinstance(simpy_container.put(1), ContainerPut)
    assert isinstance(simpy_container.get(1), ContainerGet)

    seen = {}

    def start(env):
        store = DynamicStore(env, "L", "s")
        container = DynamicContainer(env, "L", "c")
        resource = DynamicResource(env, "L", "r")
        request = resource.request()
        seen["store"] = isinstance(store, Store)
        seen["container"] = isinstance(container, Container)
        seen["request"] = isinstance(request, (Request, PriorityRequest))
        seen["priority_request"] = isinstance(request, PriorityRequest)
        seen["store_events"] = (
            isinstance(store.put("x"), StorePut),
            isinstance(store.get(), StoreGet),
        )
        seen["container_events"] = (
            isinstance(container.put(1), ContainerPut),
            isinstance(container.get(1), ContainerGet),
        )
        yield env.timeout(1)

    params = _params(
        stores={"s": CapacityConfig(current_cap=2, max_cap=2)},
        containers={"c": ContainerConfig(current_cap=2, max_cap=2, level=0)},
        resources={"r": CapacityConfig(current_cap=1, max_cap=1)},
    )
    run_low_level(params, lambda env: env.process(start(env)), until=2)

    assert seen == {
        "store": True,
        "container": True,
        "request": True,
        "priority_request": True,
        "store_events": (True, True),
        "container_events": (True, True),
    }


class _Machine:
    def __init__(self, size):
        self.size = size


def _guide_trace(machines_wanted):
    """The SimPy guide's FilterStore example: `store.items = [...]` is assigned
    before the users run, each user takes a machine of its size, and the trace is
    (time, user, machine size)."""
    env = simpy.Environment()
    shop = FilterStore(env, capacity=len(machines_wanted))
    shop.items = [_Machine(size) for size in machines_wanted]
    trace = []

    def user(name, size, at):
        yield env.timeout(at)
        machine = yield shop.get(lambda m: m.size == size)
        trace.append((env.now, name, machine.size))

    for n, size in enumerate(reversed(machines_wanted)):
        env.process(user(f"u{n}", size, n))
    env.run()
    return trace


@pytest.mark.parametrize("count", WAITING)
def test_items_setter(count):
    """`store.items = [...]` replaces what a store holds, as in the FilterStore
    example of the SimPy resources guide, and the users then take their machines
    at the same times and in the same order. A get already waiting when the items
    are assigned is served by the assignment."""
    sizes = list(range(1, count + 1))
    reference = _guide_trace(sizes)
    assert len(reference) == count

    seen = []

    def start(env):
        shop = DynamicStore(env, "L", "shop")
        shop.items = [_Machine(size) for size in sizes]

        def user(name, size, at):
            yield env.timeout(at)
            machine = yield shop.get(lambda m: m.size == size)
            seen.append((env.now, name, machine.size))

        for n, size in enumerate(reversed(sizes)):
            env.process(user(f"u{n}", size, n))
        yield env.timeout(0)

    params = _params(stores={"shop": CapacityConfig(current_cap=count, max_cap=count)})
    run_low_level(params, lambda env: env.process(start(env)), until=count + 1)

    assert seen == reference


def test_items_setter_serves_a_waiting_get_at_the_next_put():
    """A get queued on an empty store is not served by assigning `items`: as in
    SimPy it is served by the next put, and the store keeps the list given."""

    def simpy_program():
        env = simpy.Environment()
        shop = simpy.Store(env, capacity=2)
        seen = []

        def user():
            item = yield shop.get()
            seen.append((env.now, item))

        def start():
            yield env.timeout(2)
            shop.items = given
            yield env.timeout(2)
            yield shop.put("x")

        given = ["late"]
        env.process(user())
        env.process(start())
        env.run(until=6)
        return seen, shop.items is given, list(shop.items)

    reference = simpy_program()
    seen = []
    kept = []
    given = ["late"]

    def start(env):
        shop = DynamicStore(env, "L", "shop")

        def user():
            item = yield shop.get()
            seen.append((env.now, item))

        env.process(user())
        yield env.timeout(2)
        shop.items = given
        kept.append(shop.items is given)
        yield env.timeout(2)
        yield shop.put("x")
        kept.append(list(shop.items))

    params = _params(stores={"shop": CapacityConfig(current_cap=2, max_cap=2)})
    run_low_level(params, lambda env: env.process(start(env)), until=6)

    assert reference == ([(4, "late")], True, ["x"])
    assert (seen, kept[0], kept[1]) == (reference[0], reference[1], reference[2])
