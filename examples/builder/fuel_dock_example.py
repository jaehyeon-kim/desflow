"""Fuel dock: a lane of your own type, a tank that fills up and a put that is not waited for, builder API.

Fuel trucks drive into a lane at the dock, which holds two. The lane is a store of your
own type (`Lane`) that gives back the newest truck first. A crew unloads one truck every
7 seconds. A tank truck starts its pump into the tank and drives off without waiting;
the tank holds 100, so the third
pump waits for room that never comes. A marshal waits until a truck is held outside the
full lane (`putters.lane >= 1`) and then calls the overflow bay.

Needs no Docker service. Runs unpaced and ends after 40 simulation seconds.
"""

import logging

from desflow import ConsoleEgress, SimulationContext, StoreBase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)


# --8<-- [start:dock]
class Lane(StoreBase):
    """A store that gives back the newest item, like trucks parked nose to tail."""

    def _do_get(self, event):
        if self.items:
            event.succeed(self.items.pop())


app = (
    SimulationContext(sim_id="Dock", factor=0, random_seed=42)
    .add_egress(ConsoleEgress())
    # A lane of your own type; it holds two trucks
    .add_store("lane", current_cap=2, max_cap=2, type=Lane)
    # A tank of 100: two pumps fill 80 of it, so the third put waits for room
    .add_container("tank", current_cap=100, max_cap=100)
    .add_arrival(
        "diesel_trucks", dist="constant", value=3, attributes={"fuel": "diesel"}
    )
    .add_arrival(
        "petrol_trucks", dist="constant", value=5, attributes={"fuel": "petrol"}
    )
    .add_arrival("crew", dist="constant", value=7)
    .add_arrival("tankers", dist="constant", value=4)
    .add_arrival("marshals", dist="constant", value=15)
)


# The truck waits outside while the lane is full
@app.task(put={"store": "lane"})
def park(task_id, context, entity):
    return {"event_type": "truck_parked", "fuel": entity.attributes["fuel"]}


# The newest truck in the lane, because of the lane's type
@app.task(get={"store": "lane"})
def unload(task_id, context, item):
    return {"event_type": "truck_unloaded", "truck": item}


# The tank truck starts the pump and leaves; it does not wait for the put
@app.task(put={"container": "tank", "amount": 40, "wait": False})
def pump(task_id, context, entity):
    return {"event_type": "pump_started"}


# Waits until a truck is held outside the full lane
@app.task(wait={"condition": "putters.lane >= 1"})
def marshal(task_id, context, entity):
    return {"event_type": "overflow_bay_called"}


# --8<-- [end:dock]
def spawn_loop(arrival, task, name):
    """Spawns `task` for every arrival of `arrival`."""

    def loop(context):
        while True:
            entity = yield context.wait_for_arrival(arrival)
            context.spawn(task(context.next_instance(name), context, entity=entity))

    return loop


for _arrival in ("diesel_trucks", "petrol_trucks"):
    app.arrival_loop(_arrival)(spawn_loop(_arrival, park, "park"))
app.arrival_loop("crew")(spawn_loop("crew", unload, "unload"))
app.arrival_loop("tankers")(spawn_loop("tankers", pump, "pump"))
app.arrival_loop("marshals")(spawn_loop("marshals", marshal, "marshal"))


def run():
    """Runs 40 simulation seconds."""
    app.run(until=40)


if __name__ == "__main__":
    run()
