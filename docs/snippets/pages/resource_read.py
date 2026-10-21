"""Reading a resource's count, users and queue, for the Resources page.

tests/docs runs this file unpaced: the asserts check what a reader sees.
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
        sim_id="Shop", resources={"packers": CapacityConfig(current_cap=2, max_cap=4)}
    )
)
packers = DynamicResource(env, "Shop", "packers")

# --8<-- [start:low-level]
requests = [packers.request() for _ in range(4)]  # two are granted, two wait

assert packers.count == len(packers.users) == 2  # the units held now
assert packers.users == requests[:2]  # the granted requests, in the order of the grants
assert len(packers.queue) == 2  # the requests waiting
assert packers.queue[0] is requests[2]  # the next to be served
assert packers.users[0].usage_since == 0.0  # when this request was granted its unit
# --8<-- [end:low-level]

app = SimulationContext(sim_id="Shop", factor=0.0)
app.add_resource("packers", current_cap=1, max_cap=2)
app.add_service("packing", dist="constant", value=100.0)
app.add_arrival("orders", dist="constant", value=10.0)
seen = []


@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int):
    return {"event_type": "order_packed"}


@app.arrival_loop("orders")
def orders(context):
    task_id = 0
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(task_id, entity=entity))
        task_id += 1


# --8<-- [start:builder]
def look(context):
    """A process of the model: reads the packers every 30 seconds."""
    packers = context.get_resource("packers")
    while True:
        yield context.env.timeout(30)
        held = [
            request.proc.name for request in packers.users
        ]  # the tasks holding a unit
        seen.append((packers.count, len(packers.queue), held))


app.add_process(look)
# --8<-- [end:builder]

if __name__ == "__main__":
    app.run(until=95)
    assert seen == [
        (1, 1, ["pack_order"]),
        (1, 4, ["pack_order"]),
        (1, 7, ["pack_order"]),
    ]
