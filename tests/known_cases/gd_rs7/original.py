"""GD-RS7: Gas station with a monitor, a tanker and a container. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # resources.rst, the gas station: GasStation.monitor_tank, tanker, car and car_generator; env.run(35)
    env, log, L = new()
    n = [0]

    class GasStation:
        def __init__(self, env):
            self.fuel_dispensers = simpy.Resource(env, capacity=2)
            self.gas_tank = simpy.Container(env, init=100, capacity=1000)

        def monitor_tank(self, env, k):
            while True:
                L(k, "monitor", {"event_type": "checked"})
                if self.gas_tank.level < 100:
                    L(k, "call", {"event_type": "calling_tanker"})
                    n[0] += 1
                    env.process(tanker(env, self, f"tanker-{n[0]}"))
                yield env.timeout(15)
                L(k, "nap", {"event_type": "napped"})

    def tanker(env, gas_station, k):
        yield env.timeout(10)  # Need 10 Minutes to arrive
        L(k, "tanker", {"event_type": "arriving"})
        amount = gas_station.gas_tank.capacity - gas_station.gas_tank.level
        yield gas_station.gas_tank.put(amount)
        L(k, "refill", {"event_type": "refilled"})

    def car(k, env, gas_station):
        L(k, "car", {"event_type": "arriving"})
        with gas_station.fuel_dispensers.request() as req:
            yield req
            L(k, "take", {"event_type": "starts"})
            yield gas_station.gas_tank.get(40)
            yield env.timeout(5)
            L(k, "fuel", {"event_type": "done"})

    gas_station = GasStation(env)
    arrival(env, "monitor", lambda k: gas_station.monitor_tank(env, k), initial=1)

    def car_generator(env, gas_station):
        for i in range(4):
            env.process(car(f"cars-{i + 1}", env, gas_station))
            yield env.timeout(5)

    env.process(car_generator(env, gas_station))
    return go(env, log, 35)
