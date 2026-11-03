"""EX-5: Gas station with two pumps and a tank truck.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment, Sampler
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = 1000


def arrival(
    env, name, make, initial=0, gap=None, max_events=None, first=None, keyfn=None
):
    """Starts `initial` entities now, in order, then one every `gap`, the first
    `first` after the start. `make(key)` returns the process of the entity `key`."""
    count = [0]

    def key():
        count[0] += 1
        return f"{name}-{count[0]}" if keyfn is None else keyfn()

    def begin(label):
        PROCS[label] = env.process(make(label))

    for _ in range(initial):
        begin(key())
    if gap is not None:

        def source():
            made = 0
            if first is not None and first > 0:
                yield env.timeout(first)
            while max_events is None or made < max_events:
                if not (first is not None and made == 0):
                    yield env.timeout(gap() if callable(gap) else gap)
                made += 1
                begin(key())

        env.process(source())


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    PROCS.clear()
    sampler = Sampler(seed=42)
    gaps = sampler.generator("arrivals.cars.gap")
    levels = sampler.generator("arrivals.cars.attributes.level")

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    gas_station = simpy.Resource(env, 2)
    station_tank = simpy.Container(env, 200, init=200)
    n = [0]

    def keyfn():
        k = f"Car {n[0]}"
        n[0] += 1
        return k

    def car(k):
        car_tank_level = int(levels.integers(5, 26))
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

    arrival(env, "control", control, initial=size)
    arrival(env, "cars", car, gap=lambda: int(gaps.integers(30, 301)), keyfn=keyfn)
