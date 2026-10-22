"""Breakdowns and shifts: a station that breaks down and interrupts the task holding
it, capacity changed at fixed times, and signals that open and close the shop,
builder API.

A regression scenario (see scripts/regression.py), not an example. A day task opens
the shop every 150 seconds and a second task closes it 90 seconds later; an order that
arrives while the shop is closed waits up to 60 seconds for it to open, and is turned
away if it stays closed.
"""

from desflow import ConsoleEgress, SimulationContext

app = (
    SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    .add_egress(ConsoleEgress())
    .add_resource("station", current_cap=2, max_cap=4)
    .add_breakdown(
        "station",
        time_to_failure={"dist": "exponential", "rate": 0.01},
        repair={"dist": "uniform", "low": 8, "high": 20},
        work="interrupt",
    )
    .add_resource("packers", current_cap=2, max_cap=4)
    .add_signal("opened")
    .add_signal("closed")
    .add_service("packing", dist="triangular", low=2, mode=4, high=8)
    .add_service("wrapping", dist="uniform", low=2, high=4)
    .add_arrival("orders", dist="exponential", rate=0.25)
    .add_arrival("days", dist="constant", value=150)
    # Shifts: more staff for the rush, fewer after it
    .add_scenario(
        [
            {"at": 130, "path": "Shop.resources.packers.current_cap", "value": 3},
            {"at": 130, "path": "Shop.resources.station.current_cap", "value": 3},
            {"at": 310, "path": "Shop.resources.packers.current_cap", "value": 1},
            {"at": 310, "path": "Shop.resources.station.current_cap", "value": 1},
            {"at": 470, "path": "Shop.resources.packers.current_cap", "value": 2},
            {"at": 470, "path": "Shop.resources.station.current_cap", "value": 2},
        ]
    )
    .with_stats("basic", interval=70.0)
)


@app.task(signal="opened", next="close_up")
def open_shop(task_id, context):
    return {"event_type": "shop_opened"}


@app.task(wait={"timeout": 90}, signal="closed")
def close_up(task_id, context):
    return {"event_type": "shop_closed"}


@app.task(
    wait={
        "any": {
            "open": {"condition": "signal.opened.count > signal.closed.count"},
            "limit": {"timeout": 60},
        }
    },
    next={
        "when": [{"condition": "wait.open.done", "task": "pack"}],
        "default": "turned_away",
    },
)
def serve(task_id, context):
    return {"event_type": "order_received"}


@app.task(
    service_id="packing", resource_id="station", on_interrupt="resume", next="wrap"
)
def pack(task_id, context):
    return {"event_type": "order_packed"}


@app.task(service_id="wrapping", resource_id="packers")
def wrap(task_id, context):
    return {"event_type": "order_wrapped"}


@app.task()
def turned_away(task_id, context):
    return {"event_type": "order_turned_away"}


def spawn_loop(arrival, task, name):
    """Spawns `task` for every arrival of `arrival`."""

    def loop(context):
        while True:
            entity = yield context.wait_for_arrival(arrival)
            context.spawn(task(context.next_instance(name), context, entity=entity))

    return loop


app.arrival_loop("orders")(spawn_loop("orders", serve, "serve"))
app.arrival_loop("days")(spawn_loop("days", open_shop, "open_shop"))


def run():
    app.run(until=600)


if __name__ == "__main__":
    run()
