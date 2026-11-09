"""The Stores and Containers page: ranked queues, an item put by hand and the priority
of an item taken, in the Builder.

tests/docs runs this file: the model runs unpaced for 100 simulation seconds.
"""

import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DynamicRealtimeEnvironment,
    DynamicStore,
    SimParameter,
    SimulationContext,
)
from desflow.values import Template, generator

app = SimulationContext(sim_id="Shop", factor=0.0, random_seed=42).add_egress(
    ConsoleEgress()
)

# --8<-- [start:declare]
# A shelf of two, served by priority. The puts that wait for room go in rank order,
# and the latest of equal ranks goes first. The gets go in rank order too.
app.add_store(
    "returns",
    current_cap=2,
    max_cap=2,
    priority=True,
    put_queue={"rank": "attr.rank", "ties": "lifo"},
    get_queue={"rank": "attr.rank"},
)
app.add_store("archive")
# The gets and the puts that wait go in rank order, the oldest of equal ranks first
app.add_container(
    "stock",
    current_cap=10,
    max_cap=10,
    get_queue={"rank": "attr.rank"},
    put_queue={"rank": "attr.rank"},
)
# A family of two containers, crates[0] and crates[1], made when the run starts
app.add_container("crates", current_cap=5, max_cap=5, family=True, members=2)
# --8<-- [end:declare]

rank = {"rank": generator({"gen": "integer", "bounds": [1, 3]})}
# A returned parcel every second on average, and a buyer every 2 s on average
app.add_arrival(
    "returned", dist="exponential", rate=1, spawn="return_parcel", attributes=rank
)
app.add_arrival("buyers", dist="exponential", rate=0.5, spawn="buy", attributes=rank)
# A clerk every 4 s, and a delivery of 3 boxes every 5 s
app.add_arrival("clerk", dist="constant", value=4, spawn="file_return", attributes=rank)
app.add_arrival(
    "deliveries", dist="constant", value=5, spawn="deliver", attributes=rank
)

# --8<-- [start:items]
returned = Template(
    {
        "event_type": "parcel_returned",
        # putting() lists the puts that wait for room at a store or container
        "waiting": {"gen": "expr", "expr": "len(putting(returns))"},
    },
    name="tasks.return_parcel",
)


@app.task(
    # The item put on the shelf is written here, and not the payload: a mapping, with
    # a generator inside it. The priority is read as the put is made.
    put={
        "store": "returns",
        "priority": "attr.rank",
        "item": {
            "parcel": {"gen": "sequence"},
            "rank": {"gen": "attr", "name": "rank"},
        },
    }
)
def return_parcel(task_id):
    return returned


filed = Template(
    {
        "event_type": "parcel_filed",
        # item_priority is the priority of the item a priority store gave
        "priority": {"gen": "expr", "expr": "item_priority"},
    },
    name="tasks.file_return",
)


# item is the whole item the get took, and it goes into the archive as it is
@app.task(
    get={"store": "returns"},
    put={"store": "archive", "item": {"gen": "expr", "expr": "item"}},
)
def file_return(task_id):
    return filed


# --8<-- [end:items]
sold = Template(
    {
        "event_type": "boxes_sold",
        "waiting": {"gen": "expr", "expr": "len(getting(stock))"},
    },
    name="tasks.buy",
)


@app.task(get={"container": "stock", "amount": 2})
def buy(task_id):
    return sold


@app.task(put={"container": "stock", "amount": 3})
def deliver(task_id):
    return {"event_type": "boxes_delivered"}


# --8<-- [start:low-level-queue]
env = DynamicRealtimeEnvironment(factor=0.0)
env.registry.register_sim_parameter(
    SimParameter(
        sim_id="Shop", stores={"returns": CapacityConfig(current_cap=1, max_cap=1)}
    )
)
# The waiting puts go in rank order, and the latest of equal ranks goes first
shelf = DynamicStore(env, "Shop", "returns", put_ties="lifo")
taken = []


def filing(env):
    yield shelf.put("a")  # fills the shelf of one
    for parcel, rank in (("b", 2), ("c", 1), ("d", 1)):
        shelf.put(parcel, rank=rank)  # each waits for room, with its rank
    for _ in range(4):
        taken.append((yield shelf.get()))


# --8<-- [end:low-level-queue]
env.process(filing(env))
env.run()
assert taken == ["a", "d", "c", "b"]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    app.run(until=100)
