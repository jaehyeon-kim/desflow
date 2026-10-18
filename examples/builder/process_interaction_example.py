"""Process interaction: a signal, a wait with a time limit, a child flow and a failure, builder API.

A packed order is inspected by a child flow it waits for; a gift fails inspection,
and on_fail sends it to be wrapped, which ends the child flow. The order then waits
for the courier, who leaves every 50 seconds and fires the signal courier_left,
taking every order that waits; an order that has waited 40 seconds goes by post
instead.

Needs no containers. Runs unpaced and ends after 300 simulation seconds.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

app = (
    SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    .add_egress(ConsoleEgress())
    .add_resource("packers", current_cap=1, max_cap=4)
    .add_service("packing", dist="triangular", low=1, mode=2, high=4)
    .add_service("inspecting", dist="constant", value=2)
    .add_service("wrapping", dist="constant", value=3)
    .add_signal("courier_left")
    .add_arrival("orders", dist="exponential", rate=0.12, attributes={"gift": 0})
    .add_arrival("gifts", dist="exponential", rate=0.04, attributes={"gift": 1})
    .add_arrival("couriers", dist="constant", value=50)
    # Every 60 seconds, finished and failed counts so far
    .with_stats("basic", interval=60.0)
)


@app.task(service_id="packing", resource_id="packers", next="check")
def pack(task_id, context):
    return {"event_type": "order_packed"}


# --8<-- [start:check]
# Starts the child flow inspect and waits for it to end
@app.task(wait={"task": "inspect"}, next="dispatch")
def check(task_id, context):
    return {"event_type": "order_checked"}


# A gift fails inspection; on_fail sends it to wrap, and the flow goes on
@app.task(
    service_id="inspecting",
    fail="attr.gift == 1",
    fail_reason="gift needs wrapping",
    on_fail="wrap",
)
def inspect(task_id, context):
    return {"event_type": "inspected"}


@app.task(service_id="wrapping")
def wrap(task_id, context):
    return {"event_type": "wrapped"}


# --8<-- [end:check]
# --8<-- [start:dispatch]
# The courier's departure, or 40 seconds, whichever comes first
@app.task(
    wait={"any": {"courier": {"signal": "courier_left"}, "limit": {"timeout": 40}}},
    next={
        "when": [{"condition": "wait.courier.done", "task": "by_courier"}],
        "default": "by_post",
    },
)
def dispatch(task_id, context):
    return {"event_type": "dispatched"}


# --8<-- [end:dispatch]
@app.task()
def by_courier(task_id, context):
    return {"event_type": "sent_by_courier"}


@app.task()
def by_post(task_id, context):
    return {"event_type": "sent_by_post"}


# --8<-- [start:courier]
# Fires courier_left with this payload; every order waiting for it resumes
@app.task(signal="courier_left")
def courier(task_id, context):
    return {"event_type": "courier_left"}


# --8<-- [end:courier]
def spawn_loop(arrival, task, name):
    """Spawns `task` for every arrival of `arrival`."""

    def loop(context):
        while True:
            entity = yield context.wait_for_arrival(arrival)
            context.spawn(task(context.next_instance(name), context, entity=entity))

    return loop


app.arrival_loop("orders")(spawn_loop("orders", pack, "pack"))
app.arrival_loop("gifts")(spawn_loop("gifts", pack, "pack"))
app.arrival_loop("couriers")(spawn_loop("couriers", courier, "courier"))


def run():
    """Runs 300 simulation seconds."""
    app.run(until=300)


if __name__ == "__main__":
    run()
