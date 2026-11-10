"""Gas Station Refueling: SimPy's gas_station_refuel.py example, low-level API.

The low-level version of `builder/simpy/gas_station_refuel_example.py`: SimPy's own
processes on a `DynamicRealtimeEnvironment`. They publish with `publish_event` the
records that the YAML and builder versions publish: each task's `queued` and
`started`, its own events, and the number of the task's instance. A gas station has
two pumps and a tank of 200 litres. A car arrives every 30 to 300 seconds with 5 to
25 litres left in its 50 litre tank. It waits for a pump, takes the fuel it needs
from the station's tank and refuels at 2 litres a second. A control process checks
the tank every 10 seconds, and when it is below 25 percent it calls a tank truck,
which arrives 300 seconds later and fills the tank.

Each value is drawn from desflow's `Sampler`, from the random sequence the YAML and
builder versions use for the same place, so the three versions publish the same
records.

Needs no containers. Runs unpaced and ends after 1000 simulation seconds.
"""

import itertools
import logging

import simpy

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

STATION_TANK_SIZE = 200  # litres
THRESHOLD = 25  # percent of the tank that calls the truck
CAR_TANK_SIZE = 50  # litres
REFUELING_SPEED = 2  # litres a second
TANK_TRUCK_TIME = 300  # seconds
UNTIL = 1000


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the control process and the cars on `env`."""
    sampler = Sampler(seed=42)
    gaps = sampler.generator("arrivals.cars.gap")
    levels = sampler.generator("arrivals.cars.attributes.level")
    # --8<-- [start:pumps-and-tank]
    gas_station = simpy.Resource(env, 2)
    station_tank = simpy.Container(env, STATION_TANK_SIZE, init=STATION_TANK_SIZE)
    # --8<-- [end:pumps-and-tank]
    numbers = {
        task: itertools.count()
        for task in ("car", "control", "call_truck", "await_truck", "truck", "pause")
    }

    def publish(name, task, number, value):
        env.publish_event(name, value, task=task, instance=number)

    # --8<-- [start:car-refuels]
    def car(name, car_tank_level):
        number = next(numbers["car"])
        with gas_station.request() as req:
            publish(name, "car", number, {"status": "queued"})
            yield req
            fuel_required = CAR_TANK_SIZE - car_tank_level
            yield station_tank.get(fuel_required)
            publish(name, "car", number, {"status": "started"})
            yield env.timeout(fuel_required / REFUELING_SPEED)
            publish(
                name, "car", number, {"event_type": "refueled", "litres": fuel_required}
            )

    # --8<-- [end:car-refuels]

    # --8<-- [start:truck-fills-tank]
    def tank_truck(name):
        number = next(numbers["truck"])
        publish(name, "truck", number, {"status": "started"})
        yield env.timeout(TANK_TRUCK_TIME)
        amount = station_tank.capacity - station_tank.level
        yield station_tank.put(amount)
        publish(
            name, "truck", number, {"event_type": "truck_arrived", "litres": amount}
        )

    # --8<-- [end:truck-fills-tank]

    # --8<-- [start:control-calls-truck]
    def gas_station_control(name):
        while True:
            publish(
                name, "control", next(numbers["control"]), {"event_type": "checked"}
            )
            if station_tank.level / station_tank.capacity * 100 < THRESHOLD:
                call = next(numbers["call_truck"])
                publish(name, "call_truck", call, {"event_type": "truck_called"})
                waiting = next(numbers["await_truck"])
                publish(name, "await_truck", waiting, {"status": "queued"})
                yield env.process(tank_truck(name))
                publish(name, "await_truck", waiting, {"status": "started"})
                publish(name, "await_truck", waiting, {"event_type": "truck_returned"})
            pause = next(numbers["pause"])
            publish(name, "pause", pause, {"status": "started"})
            yield env.timeout(10)
            publish(name, "pause", pause, {"event_type": "waited"})

    # --8<-- [end:control-calls-truck]

    # --8<-- [start:cars-arrive]
    def car_generator():
        for i in itertools.count():
            yield env.timeout(int(gaps.integers(30, 301)))
            env.process(car(f"Car {i}", int(levels.integers(5, 26))))

    env.process(gas_station_control("control-1"))
    env.process(car_generator())
    # --8<-- [end:cars-arrive]


def run() -> None:
    """Runs 1000 simulation seconds."""
    env = DynamicRealtimeEnvironment(factor=0)
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
