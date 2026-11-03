"""GD-V4: A process is an event and its value is returned by the run.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
from typing import Any

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> Any:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    def sub(k):
        yield env.timeout(1)
        L(k, "sub", {"ret": 23})
        return 23

    def parent(k):
        ret = yield env.process(sub(k))
        L(k, "parent", {"ret": ret})
        return ret

    procs = [env.process(parent(f"a-{i + 1}")) for i in range(size)]
    return procs[0]
