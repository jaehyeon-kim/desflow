"""P36: A priority from an attribute that a step changes.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    m = simpy.PriorityResource(env, 1)
    rows = [(2.0 * (i // 2), i, 5 if i % 3 else 1) for i in range(10)]
    pass

    def job(name, t, pr, i):
        yield env.timeout(t)
        if i % 4 == 0:
            pr = 0
        with m.request(priority=pr) as r:
            yield r
            L(name, "u", {"event_type": "started"})
            yield env.timeout(1)

    for k, (t, i, pr) in enumerate(rows, 1):
        env.process(job(f"o-{k}", t, pr, i))
