"""P28: Try and catch around a step.

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
        k = f"o-{i + 1}"
        yield env.timeout(t)
        L(k, "k", S("queued"))
        with m.request() as r:
            yield r
            L(k, "k", {"status": "started"})
            try:
                yield env.timeout(1)
                if i % 2 == 0:
                    raise RuntimeError("bad")
                L(k, "k", {"event_type": "ok"})
            except RuntimeError as e:
                L(k, "k", S("failed", reason=str(e)))
                L(k, "handled", {"event_type": "handled"})

    for i in range(10):
        env.process(job(i, 2.0 * (i // 2)))
