"""EX-5: Gas station with two pumps and a tank truck. The plain SimPy program: from the SimPy program.

The three forms are the files of the example `simpy-gas-station-refuel`.
"""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets
EXAMPLE = "gas_station_refuel"  # the forms are the files of SimPy's Gas Station Refueling example


def program():
    env, log, L = new()
    gas_station = simpy.Resource(env, 2)
    station_tank = simpy.Container(env, 200, init=200)
    n = [0]

    def keyfn():
        k = f"Car {n[0]}"
        n[0] += 1
        return k

    def car(k):
        car_tank_level = 10
        with gas_station.request() as req:
            yield req
            fuel_required = 50 - car_tank_level
            yield station_tank.get(fuel_required)
            yield env.timeout(fuel_required / 2)
            L(k, "car", {"event_type": "refueled", "litres": fuel_required})

    def tank_truck(k):
        yield env.timeout(300)
        amount = station_tank.capacity - station_tank.level
        station_tank.put(amount)
        L(k, "truck", {"event_type": "truck_arrived", "litres": amount})

    def control(k):
        while True:
            L(k, "control", {"event_type": "checked"})
            if station_tank.level / station_tank.capacity * 100 < 25:
                L(k, "call_truck", {"event_type": "truck_called"})
                yield env.process(tank_truck(k))
                L(k, "await_truck", {"event_type": "truck_returned"})
            yield env.timeout(10)
            L(k, "pause", {"event_type": "waited"})

    arrival(env, "control", control, initial=1)
    arrival(env, "cars", car, gap=lambda: 60, keyfn=keyfn)
    return go(env, log, 1000)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    ("gap: {gen: integer, bounds: [30, 300]}", "gap: 60"),
    ("level: {gen: integer, bounds: [5, 25]}", "level: 10"),
]
