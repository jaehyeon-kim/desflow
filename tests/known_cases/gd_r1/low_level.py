"""GD-R1: One time unit takes a tenth of a second.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
from typing import Any
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = None
FACTOR = 0.1  # how the original makes its environment
STRICT = True  # how the original makes its environment


def build(env: DynamicRealtimeEnvironment) -> Any:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    import time

    dur = []

    def example(env, k=None, L=None):
        start = time.perf_counter()
        yield env.timeout(1)
        end = time.perf_counter()
        dur.append(end - start)
        if L:
            L(k, "one", {"event_type": "done"})

    plain = simpy.Environment()
    proc = plain.process(example(plain))
    plain.run(until=proc)
    proc = env.process(example(env, "a-1", L))
    return proc
