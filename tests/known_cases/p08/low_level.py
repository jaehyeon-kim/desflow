"""P08: Replay of a trace of arrival times.

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

    m = simpy.Resource(env, 1)

    def job(i, t):
        yield env.timeout(t)
        k = f"o-{i}"
        L(k, "k", S("queued"))
        with m.request() as r:
            yield r
            L(k, "k", {"status": "started"})
            yield env.timeout(1)
            L(k, "k", {"event_type": "served"})

    for i, t in enumerate([2.0 * (j // 2) for j in range(10)], 1):
        env.process(job(i, t))
