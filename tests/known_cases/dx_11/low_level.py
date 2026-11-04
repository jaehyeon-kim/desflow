"""DX-11: A fixed number of arrivals.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The `queued` and `started` records are published as the
blueprint publishes them, because the snippet case compares them.
"""

import simpy

from desflow import DynamicRealtimeEnvironment

UNTIL = 20


def build(env: DynamicRealtimeEnvironment) -> None:
    counter = simpy.Resource(env, 1)

    def customer(key):
        env.publish_event(key, {"status": "queued"}, task="serve")
        with counter.request() as request:
            yield request
            env.publish_event(key, {"status": "started"}, task="serve")
            yield env.timeout(3)
            env.publish_event(key, {"customer": "served"}, task="serve")

    def arrivals():
        for number in range(1, 4):
            yield env.timeout(2)
            env.process(customer(f"customers-{number}"))

    env.process(arrivals())
