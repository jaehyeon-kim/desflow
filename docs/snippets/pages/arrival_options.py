"""The Arrivals and Services page: when and how many entities an arrival starts, in
the Builder.

tests/docs runs this file: the model runs unpaced for 100 simulation seconds.
"""

import logging

from desflow import ConsoleEgress, SimulationContext
from desflow.values import generator

app = (
    SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
    .add_egress(ConsoleEgress())
    .add_resource("packers", current_cap=3, max_cap=3)
    .add_service("packing", dist="constant", value=1.5)
)

# --8<-- [start:options]
# A fixed gap, a first order at 5 s, and a number of orders drawn once: 4 on average
app.add_arrival(
    "orders",
    gap="10 s",
    first_at=5,
    max_events=generator({"gen": "sample", "dist": "geometric", "p": 0.25}),
    # A var is drawn once for each order, and the attributes and the key read it
    vars={"lane": generator({"gen": "integer", "bounds": [1, 3]})},
    attributes={"lane": generator({"gen": "expr", "expr": "lane"})},
    spawn="pack",
)
# A party of orders at a time: its size is drawn at each arrival, and 0 starts nobody
app.add_arrival(
    "parties",
    dist="exponential",
    rate=0.05,
    group=generator({"gen": "sample", "dist": "poisson", "mean": 2}),
    spawn="pack",
)
# A source that starts off, and starts a whole new gap when it is turned on
app.add_arrival(
    "rush",
    dist="exponential",
    rate=0.5,
    active=False,
    off_mode="restart",
    spawn="pack",
)
# One start at 60 s
app.add_arrival("bell", gap="1 s", first_at=60, max_events=1, spawn="open_rush")
# One start at 80 s
app.add_arrival("lull", gap="1 s", first_at=80, max_events=1, spawn="close_rush")
# Two orders that exist when the run starts, then one every 50 s: the builder always
# takes a dist or a gap
app.add_arrival("regulars", dist="constant", value=50, initial=2, spawn="pack")
# A gap of whole seconds, 4 on average
app.add_arrival("walkins", dist="geometric", p=0.25, spawn="pack")
# --8<-- [end:options]


@app.task(service_id="packing", resource_id="packers")
def pack(task_id):
    return {"event_type": "order_packed"}


# --8<-- [start:switch]
# Turns the source of rush on as the task finishes
@app.task(activate="rush")
def open_rush(task_id):
    return {"event_type": "rush_opened"}


# Turns the source of rush off again
@app.task(deactivate="rush")
def close_rush(task_id):
    return {"event_type": "rush_closed"}


# --8<-- [end:switch]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    app.run(until=100)
