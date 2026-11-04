"""DS-Y4: An event made now and used later through a local name.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

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

    def w(k):
        try:
            yield env.timeout(9)
            L(k, "w", {"event_type": "w"})
        except simpy.Interrupt as i:
            L(k, "w", S("interrupted", cause=i.cause))

    def flow(k):
        slot = m.request()
        limit = env.timeout(5, "late")
        helper = env.process(w(k))
        L(k, "begin", {"event_type": "begin"})
        yield env.timeout(1)
        L(k, "work", {"event_type": "work"})
        yield (slot | limit)
        L(k, "join", {"event_type": "join"})
        if helper.is_alive:
            L(k, "stop", {"event_type": "stop"})
            helper.interrupt("stop")
        slot.cancel()
        L(k, "end", {"event_type": "end"})
        if slot in m.users:
            m.release(slot)

    def src():
        for i in (1, 2):
            env.process(flow(f"a-{i}"))
            yield env.timeout(1)

    env.process(src())
