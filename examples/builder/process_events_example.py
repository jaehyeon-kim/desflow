"""Process events: a one-shot signal, a spawn after a delay, a subscribe and an on: action, builder API.

A guard starts an alarm round and is told when it ends (`subscribe`); a packed order
fires the `ready` signal from its own payload (an `on:` action with `fire ... from`),
which a shipper waiting for it reads; and closing time fires the one-shot `closing`
signal, whose `on:` spawns a sweep one second later (`spawn` with `after`). The
arrival gaps are set so each happens once, so the three API versions publish the same
records.

Needs no containers. Runs unpaced and ends after 12 simulation seconds.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

# --8<-- [start:events]
app = (
    SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    .add_egress(ConsoleEgress())
    .add_service("packing", dist="constant", value=2)
    .add_service("alarming", dist="constant", value=2)
    .add_service("sweeping", dist="constant", value=1)
    .add_service("guarding", dist="constant", value=100)
    # A plain signal re-arms; the one-shot closing fires once and spawns a sweep
    .add_signal("ready")
    .add_signal("closing", once=True, on=[{"spawn": {"task": "sweep", "after": 1}}])
    .add_arrival("orders", dist="constant", value=6)
    .add_arrival("shippers", dist="constant", value=7)
    .add_arrival("closers", dist="constant", value=9)
    .add_arrival("guards", dist="constant", value=8)
)


# A packed order fires ready from its own finished payload; a shipper reads it
@app.task(service_id="packing", on=[{"fire": {"signal": "ready", "from": "pack"}}])
def pack(task_id):
    return {"event_type": "order_packed"}


@app.task(wait={"signal": "ready"})
def ship(task_id):
    return {"event_type": "order_shipped"}


# Fires the one-shot closing when the shop closes
@app.task(signal="closing")
def close(task_id):
    return {"event_type": "shop_closed"}


@app.task(service_id="sweeping")
def sweep(task_id):
    return {"event_type": "floor_swept"}


# The guard starts an alarm round and is interrupted when it ends
@app.task(service_id="guarding", subscribe={"task": "alarm"}, on_interrupt="abandon")
def guard(task_id):
    return {"event_type": "shift_done"}


@app.task(service_id="alarming")
def alarm(task_id):
    return {"event_type": "alarm_rang"}


# --8<-- [end:events]


def spawn_loop(arrival, task, name):
    """Spawns `task` for every arrival of `arrival`."""

    def loop(context):
        while True:
            entity = yield context.wait_for_arrival(arrival)
            context.spawn(task(context.next_instance(name), entity=entity))

    return loop


app.arrival_loop("orders")(spawn_loop("orders", pack, "pack"))
app.arrival_loop("shippers")(spawn_loop("shippers", ship, "ship"))
app.arrival_loop("closers")(spawn_loop("closers", close, "close"))
app.arrival_loop("guards")(spawn_loop("guards", guard, "guard"))


def run():
    """Runs 12 simulation seconds."""
    app.run(until=12)


if __name__ == "__main__":
    run()
