import pytest
import simpy

from desflow import CapacityConfig, SimParameter
from desflow.resources.store import DynamicStore


def _second_store():
    return SimParameter(
        sim_id="Other", stores={"buffer2": CapacityConfig(current_cap=5, max_cap=5)}
    )


def flush_events(env):
    """Process all events scheduled for the current simulation time."""
    while env._queue and env.peek() == env.now:
        env.step()


def test_store_initialization(env, registry, sample_params):
    """Verify standard store initializes correctly as FIFO."""
    registry.register_sim_parameter(sample_params)
    store = DynamicStore(env, "Shop", "buffer1")

    assert store.capacity == 5
    assert isinstance(store._store, simpy.Store)
    assert not isinstance(store._store, simpy.PriorityStore)


def test_store_priority_initialization(env, registry, sample_params):
    """Verify priority flag correctly instantiates a PriorityStore."""
    registry.register_sim_parameter(sample_params)
    store = DynamicStore(env, "Shop", "buffer1", priority=True)

    assert store.capacity == 5
    assert isinstance(store._store, simpy.PriorityStore)


def test_priority_sorting(env, registry, sample_params):
    """Verify that priority sorting works identically to native SimPy."""
    registry.register_sim_parameter(sample_params)
    store = DynamicStore(env, "Shop", "buffer1", priority=True)
    out_log = []

    def process_flow():
        # Lower number = higher priority
        yield store.put(simpy.PriorityItem(priority=5, item="Low"))
        yield store.put(simpy.PriorityItem(priority=1, item="High"))

        first = yield store.get()
        out_log.append(first.item)

        second = yield store.get()
        out_log.append(second.item)

    env.process(process_flow())
    env.run(until=1)

    assert out_log == ["High", "Low"]


def test_dynamic_capacity_increase(env, registry, sample_params):
    """Verify increasing slots unblocks pending items."""
    # Force initial capacity to be small for this test
    sample_params.stores["buffer1"].current_cap = 2
    registry.register_sim_parameter(sample_params)

    store = DynamicStore(env, "Shop", "buffer1")

    def producer():
        yield store.put("item_1")
        yield store.put("item_2")
        yield store.put("item_3")  # Blocks here

    env.process(producer())
    env.run(until=5)

    # 3rd item is blocked
    assert len(store.items) == 2

    # Expand capacity via update
    registry.update("Shop.stores.buffer1.current_cap", 3)
    env.run(until=env.now + 0.1)

    # 3rd item unblocks and flows in
    assert store.capacity == 3
    assert len(store.items) == 3


def test_capacity_shrinkage_paradox(env, registry, sample_params):
    """Verify shrinking capacity handles over-filled buffer cleanly."""
    sample_params.stores["buffer1"].current_cap = 3
    registry.register_sim_parameter(sample_params)

    store = DynamicStore(env, "Shop", "buffer1")

    # Fill to 3
    store.put("item_1")
    store.put("item_2")
    store.put("item_3")
    env.run(until=1)

    # Shrink to 1 via update
    registry.update("Shop.stores.buffer1.current_cap", 1)
    flush_events(env)

    assert store.capacity == 1
    assert len(store.items) == 3  # Buffer is over capacity

    # Add a pending put
    store.put("item_4")
    env.run(until=5)
    assert len(store.items) == 3  # "item_4" is strictly blocked

    # Drain 3 items, making space for "item_4" since new capacity is 1
    def consumer():
        yield store.get()
        yield store.get()
        yield store.get()

    env.process(consumer())
    env.run(until=10)

    # The 3 items left, the pending 4th item finally entered (using the 1 slot)
    assert len(store.items) == 1


def test_initial_items_are_held_in_order_and_priority_pairs_by_priority(
    env, registry, sample_params
):
    registry.register_sim_parameter(sample_params)
    registry.register_sim_parameter(_second_store())
    plain = DynamicStore(env, "Shop", "buffer1", items=["a", "b"])
    ranked = DynamicStore(
        env, "Other", "buffer2", priority=True, items=[(2, "x"), (1, "y"), (2, "z")]
    )
    out = []

    def drain():
        for _ in range(2):
            out.append((yield plain.get()))
        for _ in range(3):
            out.append((yield ranked.get()).item)

    env.process(drain())
    env.run(until=1)
    # Equal priorities keep the order they were given in
    assert out == ["a", "b", "y", "x", "z"]


def test_initial_items_above_capacity_or_without_a_priority_are_refused(
    env, registry, sample_params
):
    registry.register_sim_parameter(sample_params)
    registry.register_sim_parameter(_second_store())
    with pytest.raises(ValueError, match="cannot start with 6"):
        DynamicStore(env, "Shop", "buffer1", items=list(range(6)))
    with pytest.raises(ValueError, match="pairs"):
        DynamicStore(env, "Other", "buffer2", priority=True, items=["x"])


def test_a_priority_store_refuses_a_filter(env, registry, sample_params):
    registry.register_sim_parameter(sample_params)
    store = DynamicStore(env, "Shop", "buffer1", priority=True)
    with pytest.raises(ValueError, match="both ranks and filters"):
        store.get(filter=lambda item: True)


def test_a_store_and_a_container_start_at_capacity_0():
    """SimPy refuses a capacity of 0; desflow accepts it, as for a resource, and a
    put waits until the capacity rises."""
    from desflow import ContainerConfig, DynamicContainer, DynamicRealtimeEnvironment

    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            stores={"shelf": CapacityConfig(current_cap=0, max_cap=2)},
            containers={"tank": ContainerConfig(current_cap=0, max_cap=10, level=0)},
        )
    )
    shelf = DynamicStore(env, "Shop", "shelf")
    tank = DynamicContainer(env, "Shop", "tank")
    assert (shelf.capacity, tank.capacity) == (0, 0)
    done = []

    def fill():
        yield shelf.put("box")
        done.append(("box", env.now))
        yield tank.put(4)
        done.append(("oil", env.now))

    def grow():
        yield env.timeout(5)
        env.registry.update("Shop.stores.shelf.current_cap", 1)
        yield env.timeout(5)
        env.registry.update("Shop.containers.tank.current_cap", 10)

    env.process(fill())
    env.process(grow())
    env.run(until=20)
    env.teardown()
    assert done == [("box", 5), ("oil", 10)]


def test_a_priority_store_gives_equal_priorities_in_put_order():
    """Six items of one priority come out in put order. A `PriorityItem` compares
    only its priority, so without the put order heapq returns them mixed."""
    from tests.conformance.test_c5_priority_store import _builder_order

    assert _builder_order([3] * 6) == [0, 1, 2, 3, 4, 5]
