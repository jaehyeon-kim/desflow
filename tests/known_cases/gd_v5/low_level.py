"""GD-V5: A process started after a delay, then waited for.

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

    from simpy.util import start_delayed

    def sub(k):
        L(k, "delayed_sub", {"event_type": "starting"})
        yield env.timeout(1)
        L(k, "sub", {"ret": 23})
        return 23

    def parent(k):
        sub_proc = yield start_delayed(env, sub(k), delay=3)
        ret = yield sub_proc
        L(k, "parent", {"event_type": "parent_done", "ret": ret})
        return ret

    procs = [env.process(parent(f"a-{i + 1}")) for i in range(size)]
    return procs[0]
