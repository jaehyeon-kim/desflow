"""ST-R06: A process that asks again does not pass the queue.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    def child(env, res, k):
        result = []
        for _ in range(3):
            with res.request() as req:
                yield req
                result.append(env.now)
                yield env.timeout(1)
                L(k, "round", {"event_type": "acquired"})
        return result

    def parent(env):
        res = simpy.Resource(env, 1)
        child_a = env.process(child(env, res, "children-1"))
        child_b = env.process(child(env, res, "children-2"))
        yield child_a
        yield child_b

    env.process(parent(env))
