"""The resources, containers and stores of the Resources page.

A complete low-level run: tests/unit runs it unpaced, so every call is checked.
"""

from dynamic_des import (
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
        sim_id="Line_A",
        resources={"lathe": CapacityConfig(current_cap=2, max_cap=5)},
        containers={"coolant": CapacityConfig(current_cap=100.0, max_cap=200.0)},
        stores={"finished_goods": CapacityConfig(current_cap=50, max_cap=100)},
    )
)

# --8<-- [start:resource]
lathe = DynamicResource(env, "Line_A", "lathe")  # capacity from the registry: 2


def part(env, part_id, priority=1):
    with lathe.request(priority=priority) as request:  # a lower number goes first
        yield request  # waits until a unit is free
        yield env.timeout(10.0)  # holds the unit for 10 s
    # leaving the with block releases the unit


# --8<-- [end:resource]


# --8<-- [start:shrink]
def breakdown(env):
    """Takes 3 of 5 lathes out of service at t=5 s. Running parts are not stopped."""
    env.registry.update("Line_A.resources.lathe.max_cap", 5)
    env.registry.update("Line_A.resources.lathe.current_cap", 5)
    yield env.timeout(5.0)
    env.registry.update("Line_A.resources.lathe.current_cap", 2)
    yield env.timeout(0.1)  # the resource applies the change in its own process
    print(f"capacity {lathe.capacity}, in use {lathe.in_use}")  # capacity 2, in use 4


# --8<-- [end:shrink]

# --8<-- [start:container]
coolant = DynamicContainer(env, "Line_A", "coolant", init=80.0)  # 80 of 100 litres


def cool(env):
    yield coolant.get(5.0)  # takes 5 litres, waiting if fewer are left
    yield coolant.put(5.0)  # puts them back, waiting if the tank is full
    print(f"coolant level {coolant.level}")


# --8<-- [end:container]

# --8<-- [start:store]
goods = DynamicStore(env, "Line_A", "finished_goods")  # at most 50 items


def pack(env, part_id):
    yield goods.put({"part_id": part_id})  # waits while the store is full
    item = yield goods.get()  # waits while the store is empty
    print(f"shipped {item['part_id']}")


# --8<-- [end:store]

for i in range(4):  # four parts, all running when the breakdown comes
    env.process(part(env, i))
env.process(breakdown(env))
env.process(cool(env))
env.process(pack(env, 1))

if __name__ == "__main__":
    try:
        env.run(until=20.0)
    finally:
        env.teardown()
    assert lathe.capacity == 2
    assert lathe.in_use == 0
