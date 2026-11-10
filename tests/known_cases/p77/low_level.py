"""P77: Release all units at once.

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

    n = size
    m = simpy.Resource(env, 3)

    def p(i):
        k = f"a-{i}"
        reqs = []
        for name in ("one", "two"):
            r = m.request()
            yield r
            reqs.append(r)
            L(k, "take_one" if name == "one" else "take_two", {"event_type": name})
        yield env.timeout(1)
        L(k, "work", {"event_type": "worked"})
        while True:
            L(k, "release_all", {"event_type": "releasing"})
            m.release(reqs.pop(0))
            if not reqs:
                break

    for i in range(1, n + 1):
        env.process(p(i))
