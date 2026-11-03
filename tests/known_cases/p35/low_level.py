"""P35: Change the queue limit at run time.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = 7


def S(status, **fields):
    """A record value that carries a status."""
    return {"status": status, **fields}


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    m = simpy.Resource(env, 1)
    limit = [1]

    def scenario():
        yield env.timeout(2)
        limit[0] = 3

    env.process(scenario())

    def job(k):
        if len(m.queue) >= limit[0]:
            L(k, "job", S("balked", resource="m"))
            return
        with m.request() as r:
            yield r
            yield env.timeout(5)
            L(k, "job", {"event_type": "done"})

    def src():
        for i, t in enumerate((0, 1, 3, 3.5), 1):
            if t > env.now:
                yield env.timeout(t - env.now)
            env.process(job(f"a-{i}"))

    env.process(src())
