"""The resources of the Resources page: a low-level run, and a builder task.

tests/docs runs this file unpaced, so every call is checked, and the page shows what
it prints: the order each queue order serves four requests in, and a capacity drop.
"""

from desflow import (
    CapacityConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    SimParameter,
    SimulationContext,
)

env = DynamicRealtimeEnvironment(factor=0.0)
env.registry.register_sim_parameter(
    SimParameter(
        sim_id="Shop",
        resources={
            "packers": CapacityConfig(current_cap=2, max_cap=5),
            "benches": CapacityConfig(current_cap=1, max_cap=1),
        },
    )
)

# --8<-- [start:resource]
packers = DynamicResource(env, "Shop", "packers")  # capacity from the registry: 2


def order(env, order_id):
    with packers.request() as request:
        yield request  # waits until a unit is free
        yield env.timeout(10.0)  # holds the unit for 10 s
    # leaving the with block releases the unit


# --8<-- [end:resource]


# --8<-- [start:queue-order]
def served_order(queue):
    """Four requests join while the only bench is busy; returns who is served when."""
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            resources={"benches": CapacityConfig(current_cap=1, max_cap=1)},
        )
    )
    bench = DynamicResource(env, "Shop", "benches", queue=queue)
    served = []

    def use(name, priority, after=0.0):
        yield env.timeout(after)
        # The priority counts only under the priority order
        with bench.request(priority=priority) as request:
            yield request
            served.append(name)
            yield env.timeout(1.0)

    env.process(use("first", 1))  # takes the bench at once
    for name, priority in [
        ("orders-1", 1),
        ("express-1", 0),
        ("orders-2", 1),
        ("express-2", 0),
    ]:
        env.process(use(name, priority, after=0.1))  # all join while it is busy
    env.run(until=10)
    return served[1:]


for queue in ("fifo", "lifo", "priority"):
    print(f"{queue:<8} {', '.join(served_order(queue))}")
# --8<-- [end:queue-order]

# --8<-- [start:declare]
app = SimulationContext(sim_id="Shop", factor=0.0)
# fifo (the default), lifo or priority
app.add_resource("packers", current_cap=1, max_cap=4, queue="priority")
# --8<-- [end:declare]

# --8<-- [start:priority]
app = (
    SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=4, queue="priority")
    .add_service("packing", dist="constant", value=1.5)
    .add_arrival("orders", dist="exponential", rate=0.5, attributes={"tier": 2})
)


# Each order's own priority, from its attributes, when it joins the queue;
# a number or an expression such as "attr.tier" works too
@app.task(
    service_id="packing",
    resource_id="packers",
    priority=lambda task_id, attributes: attributes["tier"],
)
def pack_order(task_id: int):
    return {"event_type": "order_packed"}


# --8<-- [end:priority]


@app.arrival_loop("orders")
def orders(context):
    task_id = 0
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(task_id, entity=entity))
        task_id += 1


# --8<-- [start:shrink]
def capacity_drop(env):
    """Takes 3 of 5 packers off the floor at t=5 s. Orders being packed are not stopped."""
    env.registry.update("Shop.resources.packers.current_cap", 5)
    yield env.timeout(5.0)
    env.registry.update("Shop.resources.packers.current_cap", 2)
    yield env.timeout(0.1)  # the resource applies the change in its own process
    print(f"capacity {packers.capacity}, in use {packers.in_use}")


# --8<-- [end:shrink]

for i in range(4):  # four orders, all being packed when the change comes
    env.process(order(env, i))
env.process(capacity_drop(env))

if __name__ == "__main__":
    try:
        env.run(until=20.0)
    finally:
        env.teardown()
    assert packers.capacity == 2
    assert packers.in_use == 0
    app.run(until=60)
