"""DX-9: A service time computed for each car.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The `queued` and `started` records are published as the
blueprint publishes them, because the snippet case compares them.
"""

import simpy

from desflow import DynamicRealtimeEnvironment

UNTIL = 10


def build(env: DynamicRealtimeEnvironment) -> None:
    pump = simpy.Resource(env, 1)

    def car(key, name, litres):
        env.publish_event(key, {"status": "queued"}, task="fill")
        with pump.request() as request:
            yield request
            env.publish_event(key, {"status": "started"}, task="fill")
            yield env.timeout(litres / 2)
            env.publish_event(key, {"car": name}, task="fill")

    def arrival(label, first, name, litres):
        yield env.timeout(first)
        env.process(car(f"{label}-1", name, litres))

    for label, first, name, litres in (
        ("car1", 0, "car-1", 4),
        ("car2", 1, "car-2", 6),
        ("car3", 2, "car-3", 2),
    ):
        env.process(arrival(label, first, name, litres))
