"""CV-134: A computed list of events made now and waited for later.

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

    rs = [simpy.Resource(env, capacity=1) for _ in range(3)]

    def job(k):
        yield env.timeout(0.5)
        L(k, "begin", {"e": "half"})
        pool = [r.request() for r in rs]
        L(k, "opener", {"e": "opened"})
        yield pool[2]
        L(k, "waiter", {"e": "got"})
        for request in pool:
            if not request.triggered:
                request.cancel()
        flags = [request.processed for request in pool]
        L(k, "closer", {"e": "done", "x": [flags, [len(r.queue) for r in rs]]})
        rs[2].release(pool[2])

    def hold(k, i):
        with rs[i].request() as request:
            yield request
            yield env.timeout(3 - i)
        L(k, "hold", {"e": "held"})

    for n in range(1, size + 1):
        PROCS[f"a-{n}"] = env.process(job(f"a-{n}"))
    for i in range(3):
        PROCS[f"h-{i + 1}"] = env.process(hold(f"h-{i + 1}", i))
