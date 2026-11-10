"""GD-RS7: Gas station with a monitor, a tanker and a container.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = 35


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

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

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
        yield env.timeout(10)
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
    arrival(env, "monitor", lambda k: gas_station.monitor_tank(env, k), initial=size)

    def car_generator(env, gas_station):
        for i in range(4):
            env.process(car(f"cars-{i + 1}", env, gas_station))
            yield env.timeout(5)

    env.process(car_generator(env, gas_station))
