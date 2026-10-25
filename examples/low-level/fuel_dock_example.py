"""Fuel dock: a lane of your own type, a tank that fills up and a put that is not waited for, low-level API.

The low-level version of `builder/fuel_dock_example.py`. Fuel trucks drive into a lane at
the dock, which holds two. The lane is a store of your own type (`Lane`) that gives
back the newest truck first. A crew unloads one truck every 7 seconds. A tank truck
starts its pump into the tank and drives off without waiting; the tank holds 100, so the
third pump waits for room that never comes. A marshal waits until a truck is held
outside the full lane (`putters_waiting` is above 0) and then calls the overflow bay.

Needs no Docker service. Runs unpaced and ends after 40 simulation seconds.
"""

import itertools
import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    ContainerConfig,
    DistributionConfig,
    DynamicContainer,
    DynamicRealtimeEnvironment,
    DynamicStore,
    Sampler,
    SimParameter,
    StoreBase,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

PARAMS = SimParameter(
    sim_id="Dock",
    arrival={
        "diesel_trucks": DistributionConfig(dist="constant", value=3),
        "petrol_trucks": DistributionConfig(dist="constant", value=5),
        "crew": DistributionConfig(dist="constant", value=7),
        "tankers": DistributionConfig(dist="constant", value=4),
        "marshals": DistributionConfig(dist="constant", value=15),
    },
    stores={"lane": CapacityConfig(current_cap=2, max_cap=2)},
    # A tank of 100: two pumps fill 80 of it, so the third put waits for room
    containers={"tank": ContainerConfig(current_cap=100, max_cap=100)},
)
FUEL = {"diesel_trucks": "diesel", "petrol_trucks": "petrol"}


# --8<-- [start:dock]
class Lane(StoreBase):
    """A store that gives back the newest item, like trucks parked nose to tail."""

    def _do_get(self, event):
        if self.items:
            event.succeed(self.items.pop())


def build(env):
    """Builds the lane and the tank, in the builder's order."""
    return {
        # A lane of your own type; it holds two trucks
        "lane": DynamicStore(env, "Dock", "lane", type=Lane),
        # The tank holds 100, the capacity in the parameters
        "tank": DynamicContainer(env, "Dock", "tank"),
    }


# --8<-- [end:dock]
def park(env, dock, key, fuel, task_id):
    """The truck waits outside while the lane is full, then publishes."""
    payload = {"event_type": "truck_parked", "fuel": fuel}
    yield dock["lane"].put(payload)
    env.publish_event(key, payload, task="park", instance=task_id)


def unload(env, dock, key, task_id):
    """Takes the newest truck in the lane, because of the lane's type."""

    def publish(value):
        env.publish_event(key, value, task="unload", instance=task_id)

    publish({"status": "queued"})
    truck = yield dock["lane"].get()
    publish({"status": "started"})
    publish({"event_type": "truck_unloaded", "truck": truck})


def pump(env, dock, key, task_id):
    """Starts the pump and leaves; the put is not yielded, so nothing waits for it."""
    dock["tank"].put(40)
    env.publish_event(
        key, {"event_type": "pump_started"}, task="pump", instance=task_id
    )
    yield env.timeout(0)


def marshal(env, dock, key, task_id):
    """Waits until a truck is held outside the full lane, then calls the bay."""

    def publish(value):
        env.publish_event(key, value, task="marshal", instance=task_id)

    publish({"status": "queued"})
    held = env.event()

    def check():
        if dock["lane"].putters_waiting >= 1 and not held.triggered:
            held.succeed()

    check()
    if not held.triggered:
        # queues=True: the length of the put queue is what the check reads
        env.watch_changes(check, queues=True)
        yield held
        env.unwatch_changes(check)
    publish({"status": "started"})
    publish({"event_type": "overflow_bay_called"})


def generator(env, sampler, dock, arrival, task, ids):
    """Spawns `task` for every arrival, keyed diesel_trucks-1 and so on."""
    gap = env.registry.get_config(f"Dock.arrival.{arrival}")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        key = f"{arrival}-{number}"
        extra = (FUEL[arrival],) if arrival in FUEL else ()
        env.process(task(env, dock, key, *extra, next(ids)))


def processes(env, sampler, dock):
    """The five arrivals, in the builder's order."""
    parks = itertools.count()
    return [
        generator(env, sampler, dock, "diesel_trucks", park, parks),
        generator(env, sampler, dock, "petrol_trucks", park, parks),
        generator(env, sampler, dock, "crew", unload, itertools.count()),
        generator(env, sampler, dock, "tankers", pump, itertools.count()),
        generator(env, sampler, dock, "marshals", marshal, itertools.count()),
    ]


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    sampler = Sampler(seed=42)
    dock = build(env)
    for process in processes(env, sampler, dock):
        env.process(process)
    try:
        env.run(until=40)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
