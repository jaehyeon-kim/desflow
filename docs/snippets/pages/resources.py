"""The resources, containers and stores of the Resources page.

A complete low-level run: tests/unit runs it unpaced, so every call is checked.
"""

from desflow import (
    CapacityConfig,
    DynamicContainer,
    DynamicRealtimeEnvironment,
    DynamicResource,
    DynamicStore,
    SimParameter,
)

env = DynamicRealtimeEnvironment(factor=0.0)
env.registry.register_sim_parameter(
    SimParameter(
        sim_id="Shop",
        resources={"packers": CapacityConfig(current_cap=2, max_cap=5)},
        containers={"boxes": CapacityConfig(current_cap=100.0, max_cap=200.0)},
        stores={"dispatch": CapacityConfig(current_cap=50, max_cap=100)},
    )
)

# --8<-- [start:resource]
packers = DynamicResource(env, "Shop", "packers")  # capacity from the registry: 2


def order(env, order_id, priority=1):
    with packers.request(priority=priority) as request:  # a lower number goes first
        yield request  # waits until a unit is free
        yield env.timeout(10.0)  # holds the unit for 10 s
    # leaving the with block releases the unit


# --8<-- [end:resource]


# --8<-- [start:shrink]
def breakdown(env):
    """Takes 3 of 5 packers off the floor at t=5 s. Orders being packed are not stopped."""
    env.registry.update("Shop.resources.packers.max_cap", 5)
    env.registry.update("Shop.resources.packers.current_cap", 5)
    yield env.timeout(5.0)
    env.registry.update("Shop.resources.packers.current_cap", 2)
    yield env.timeout(0.1)  # the resource applies the change in its own process
    print(
        f"capacity {packers.capacity}, in use {packers.in_use}"
    )  # capacity 2, in use 4


# --8<-- [end:shrink]

# --8<-- [start:container]
boxes = DynamicContainer(env, "Shop", "boxes", init=80.0)  # 80 of 100 boxes


def cool(env):
    yield boxes.get(5.0)  # takes 5 boxes, waiting if fewer are left
    yield boxes.put(5.0)  # puts them back, waiting if the tank is full
    print(f"boxes level {boxes.level}")


# --8<-- [end:container]

# --8<-- [start:store]
goods = DynamicStore(env, "Shop", "dispatch")  # at most 50 items


def pack(env, order_id):
    yield goods.put({"order_id": order_id})  # waits while the store is full
    item = yield goods.get()  # waits while the store is empty
    print(f"shipped {item['order_id']}")


# --8<-- [end:store]

for i in range(4):  # four orders, all being packed when the change comes
    env.process(order(env, i))
env.process(breakdown(env))
env.process(cool(env))
env.process(pack(env, 1))

if __name__ == "__main__":
    try:
        env.run(until=20.0)
    finally:
        env.teardown()
    assert packers.capacity == 2
    assert packers.in_use == 0
