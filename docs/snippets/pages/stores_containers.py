"""The stores and containers of the Stores and Containers page: builder and low-level.

tests/docs runs this file: the builder model runs unpaced for a minute, and the
low-level part takes from and adds to a container and a store and checks the levels.
"""

from desflow import (
    CapacityConfig,
    ContainerConfig,
    DynamicContainer,
    DynamicRealtimeEnvironment,
    DynamicStore,
    SimParameter,
    SimulationContext,
)

app = (
    SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
    .add_resource("packers", current_cap=2, max_cap=4)
    .add_service("packing", dist="constant", value=3)
    .add_service("collecting", dist="constant", value=1)
    .with_stats("minimal")  # capacity, level, getters and putters waiting
)
# --8<-- [start:builder-container]
# An amount: room for 100 boxes, 20 when the run starts
app.add_container("stock", current_cap=100, max_cap=100, level=20)
# --8<-- [end:builder-container]
# --8<-- [start:builder-store]
# Items: room for 5 parcels, empty when the run starts
app.add_store("parcels", current_cap=5, max_cap=10)
# --8<-- [end:builder-store]


@app.task(next="pack_order")
def take_order(task_id: int):
    """Takes no time; its payload says how many boxes the order needs."""
    return {"boxes": 2}


# --8<-- [start:builder-take]
# Takes the boxes the previous task's payload names, once it has a packer.
# The amount is a number, an expression, or a function of the task id and the
# previous task's payload, such as lambda task_id, payload: payload["boxes"].
@app.task(
    service_id="packing",
    resource_id="packers",
    get={"container": "stock", "amount": "payload.boxes"},
    put={"store": "parcels"},  # adds the finished payload as an item
)
def pack_order(task_id: int):
    return {"event_type": "order_packed"}


# Takes the oldest parcel, waiting while the store is empty; `item` is that parcel
@app.task(service_id="collecting", get={"store": "parcels"})
def collect(task_id: int, item):
    return {"event_type": "parcel_collected", "parcel": item["event_type"]}


# --8<-- [end:builder-take]


# --8<-- [start:builder-level]
def stock_count(context: SimulationContext):
    """A stock count at 30 seconds: the level path sets what the container holds."""
    yield context.env.timeout(30)
    context.env.registry.update("Shop.containers.stock.level", 50)


app.add_process(stock_count)
# --8<-- [end:builder-level]


# --8<-- [start:low-level-container]
env = DynamicRealtimeEnvironment(factor=0.0)
env.registry.register_sim_parameter(
    SimParameter(
        sim_id="Shop",
        # An amount: room for 100 boxes, 20 when the run starts
        containers={"stock": ContainerConfig(current_cap=100, max_cap=100, level=20)},
        # Items: room for 5 parcels, empty when the run starts
        stores={"parcels": CapacityConfig(current_cap=5, max_cap=10)},
    )
)
stock = DynamicContainer(env, "Shop", "stock")  # starts at the level above, 20
parcels = DynamicStore(env, "Shop", "parcels")
# --8<-- [end:low-level-container]


# --8<-- [start:low-level-take]
def pack(env, order_id):
    yield stock.get(2)  # takes 2 boxes, waiting while fewer are left
    yield parcels.put({"order_id": order_id})  # waits while the store is full


def collect_parcel(env):
    parcel = yield parcels.get()  # waits while the store is empty
    print(f"collected order {parcel['order_id']}; {stock.level:g} boxes left")


# --8<-- [end:low-level-take]


# --8<-- [start:low-level-level]
def stock_count_process(env):
    """A stock count: sets the level; waiting gets and puts are woken."""
    yield env.timeout(30)
    env.registry.update("Shop.containers.stock.level", 50)


# --8<-- [end:low-level-level]

env.process(pack(env, 1))
env.process(collect_parcel(env))
env.process(stock_count_process(env))


def spawn(arrival, task, gap):
    def loop(context):
        task_id = 0
        while True:
            entity = yield context.wait_for_arrival(arrival)
            context.spawn(task(task_id, entity=entity))
            task_id += 1

    app.add_arrival(arrival, **gap)
    app.arrival_loop(arrival)(loop)


spawn("orders", take_order, {"dist": "exponential", "rate": 0.5})
spawn("couriers", collect, {"dist": "constant", "value": 4})

if __name__ == "__main__":
    try:
        env.run(until=60)
    finally:
        env.teardown()
    assert stock.level == 50 and len(parcels.items) == 0
    app.run(until=60)
