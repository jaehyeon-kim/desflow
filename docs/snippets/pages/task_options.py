"""The Tasks page: how an order leaves the line, and what a task does when it is
interrupted, in the Builder.

tests/docs runs this file: the model runs unpaced for 3 simulation minutes.
"""

import logging

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

app = (
    SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
    .add_egress(ConsoleEgress())
    .add_resource("packers", current_cap=1, max_cap=3, queue="priority", preempt=True)
    .add_service("packing", dist="constant", value=3)
    .add_service("labelling", dist="constant", value=4)
    .add_service("repairing", dist="constant", value=1)
    .add_arrival("orders", dist="constant", value=2, spawn="pack_order")
    .add_arrival("rush", dist="constant", value=20, spawn="rush_pack")
    .add_arrival("gifts", dist="constant", value=25, spawn="pack_gift")
    .add_arrival("labels", dist="constant", value=8, spawn="print_label")
    .add_scenario([{"at": 10, "interrupt": {"task": "print_label", "cause": "jam"}}])
)


# --8<-- [start:leave]
@app.task(
    service_id="packing",
    resource_id="packers",
    priority=1,
    patience="8 s",  # the order leaves the line when it has waited 8 seconds
    balk="t >= 150",  # the order does not join the line from 150 seconds on
    # Where an order goes after it leaves the line; it reads upstream.status
    on_renege="follow_up",
    on_balk="follow_up",
)
def pack_order(task_id):
    return {"event_type": "order_packed"}


order_left = Template(
    {"event_type": "order_left", "how": {"gen": "expr", "expr": "upstream.status"}},
    name="tasks.follow_up",
)


@app.task()
def follow_up(task_id):
    return order_left


# --8<-- [end:leave]


@app.task(service_id="packing", resource_id="packers", priority=0)
def rush_pack(task_id):
    return {"event_type": "rush_packed"}


# --8<-- [start:abandon]
@app.task(
    service_id="packing",
    resource_id="packers",
    priority=1,
    # A gift that loses its packer gives it up and is packed by hand instead
    on_interrupt="abandon",
    on_interrupt_next="pack_by_hand",
)
def pack_gift(task_id):
    return {"event_type": "gift_packed"}


@app.task(service_id="packing")
def pack_by_hand(task_id):
    return {"event_type": "packed_by_hand"}


# --8<-- [end:abandon]


# --8<-- [start:run]
@app.task(
    service_id="labelling",
    # After an interrupt, retool runs as a child flow of the same entity, and then
    # the label goes on with the time it had left
    on_interrupt="resume",
    on_interrupt_run="retool",
)
def print_label(task_id):
    return {"event_type": "label_printed"}


retooled = Template(
    {"event_type": "retooled", "left": {"gen": "expr", "expr": "cause.left"}},
    name="tasks.retool",
)


@app.task(service_id="repairing")
def retool(task_id):
    return retooled


# --8<-- [end:run]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    app.run(until=180)
