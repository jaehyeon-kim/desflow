"""CV-071: Two units held by one process.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    m, n = simpy.Resource(env, capacity=2), simpy.Resource(env, capacity=2)

    def seizes(k):
        first = m.request()
        yield first
        L(k, "one", {"e": "one", "x": m.count})
        second = m.request()
        yield second
        L(k, "two", {"e": "both", "x": m.count})
        yield env.timeout(1)
        L(k, "first", {"e": "first"})
        m.release(first)
        L(k, "after_first", {"e": "after_first", "x": m.count})
        yield env.timeout(1)
        L(k, "second", {"e": "second"})
        m.release(second)
        L(k, "after_second", {"e": "after_second", "x": m.count})

    def named(k):
        p, q = n.request(), n.request()
        yield p & q
        L(k, "subject", {"e": "both", "x": n.count})
        yield env.timeout(1)
        L(k, "free_q", {"e": "free_q"})
        n.release(q)
        L(k, "after_q", {"e": "after_q", "x": [n.count, p in n.users]})
        yield env.timeout(1)
        L(k, "free_p", {"e": "free_p"})
        n.release(p)
        L(k, "after_p", {"e": "after_p", "x": n.count})

    for i in range(1, size + 1):
        PROCS[f"a-{i}"] = env.process(seizes(f"a-{i}"))
    PROCS["b-1"] = env.process(named("b-1"))
