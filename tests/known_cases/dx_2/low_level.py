"""DX-2: A lane that gives back the newest truck.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = 40


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


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    lane = simpy.PriorityStore(env, capacity=2)
    tank = simpy.Container(env, capacity=100)
    parked = [0]
    changed = [env.event()]

    def poke():
        ev = changed[0]
        changed[0] = env.event()
        ev.succeed()

    def number(fuel):

        def f(k):
            seq = parked[0] + 1
            parked[0] = parked[0] + 1
            pr = simpy.PriorityItem(-seq, {"event_type": "truck_parked", "fuel": fuel})
            g = lane.put(pr)
            if not g.triggered:
                poke()
            yield g
            L(k, "park", {"event_type": "truck_parked", "fuel": fuel})
            poke()

        return f

    def unload(k):
        L(k, "unload", {"status": "queued"})
        it = yield lane.get()
        L(k, "unload", {"status": "started"})
        poke()
        L(k, "unload", {"event_type": "truck_unloaded", "truck": it.item})

    def pump(k):
        if tank.level + 40 <= 100:
            tank.put(40)
            L(k, "pump", {"event_type": "pump_started"})
        else:
            L(k, "pump", {"event_type": "pump_started"})
        yield env.timeout(0)

    def marshal(k):
        L(k, "marshal", {"status": "queued"})
        L(k, "marshal", {"status": "started"})
        e = env.event()
        if len(lane.put_queue) >= 1:
            e.succeed()
        yield e
        while not len(lane.put_queue) >= 1:
            yield changed[0]
        L(k, "marshal", {"event_type": "overflow_bay_called"})

    arrival(env, "diesel_trucks", number("diesel"), gap=3)
    arrival(env, "petrol_trucks", number("petrol"), gap=5)
    arrival(env, "crew", unload, gap=7)
    arrival(env, "tankers", pump, gap=4)
    arrival(env, "marshals", marshal, gap=15)
