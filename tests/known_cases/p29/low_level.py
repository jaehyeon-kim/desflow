"""P29: Seize two units of a pool.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def S(status, **fields):
    """A record value that carries a status."""
    return {"status": status, **fields}


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    c = simpy.Container(env, 3, init=3)

    def j(i, t0, n):
        k = f"a-{i}"
        yield env.timeout(t0)
        L(k, "job", S("queued"))
        yield c.get(n)
        L(k, "job", {"status": "started"})
        yield env.timeout(1)
        yield c.put(n)
        L(k, "job", {"event_type": "served"})

    for i, (t0, n) in enumerate([(0, 2), (0.5, 2), (0.6, 1)], 1):
        env.process(j(i, t0, n))
