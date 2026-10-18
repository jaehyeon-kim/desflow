"""A priority store that starts with items, for the Stores and Containers page.

tests/docs runs this file: the builder and the low-level store each hold two parcels
before the run, and a courier takes the express one first.
"""

import simpy

from desflow import (
    CapacityConfig,
    DynamicRealtimeEnvironment,
    DynamicStore,
    SimParameter,
    SimulationContext,
)

# --8<-- [start:builder-store]
app = SimulationContext(sim_id="Shop", factor=0.0)
# A courier takes the lowest priority first, the oldest first among equals.
# Left from yesterday: (priority, item) pairs, held before the first put.
app.add_store(
    "parcels",
    current_cap=10,
    max_cap=10,
    priority=True,
    items=[(2, {"order": "standard-0"}), (1, {"order": "express-0"})],
)
# --8<-- [end:builder-store]
collected = []


@app.task(get={"store": "parcels"})
def collect(task_id: int, item):
    collected.append(item["order"])
    return {"event_type": "parcel_collected"}


def courier(context):
    yield context.env.timeout(2)
    context.spawn(collect(0))


app.add_process(courier)
app.run(until=5)
assert collected == ["express-0"]

env = DynamicRealtimeEnvironment(factor=0.0)
env.registry.register_sim_parameter(
    SimParameter(
        sim_id="Shop", stores={"parcels": CapacityConfig(current_cap=10, max_cap=10)}
    )
)
# --8<-- [start:low-level-store]
parcels = DynamicStore(
    env,
    "Shop",
    "parcels",
    priority=True,
    # Each pair is held as a PriorityItem, ranked by priority and then by order
    items=[(2, {"order": "standard-0"}), (1, {"order": "express-0"})],
)


def courier_process(env):
    first = yield parcels.get()  # a PriorityItem; .item is the parcel
    print(f"collected {first.item['order']}")
    # A put of your own wraps the item the same way, so ties keep their order
    yield parcels.put(parcels.wrap({"order": "express-1"}, 1))


# --8<-- [end:low-level-store]
env.process(courier_process(env))
env.run(until=1)
assert [p.item["order"] for p in sorted(parcels.items)] == ["express-1", "standard-0"]
assert isinstance(parcels.items[0], simpy.PriorityItem)
