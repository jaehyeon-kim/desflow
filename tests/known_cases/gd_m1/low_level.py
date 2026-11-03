"""GD-M1: Collect a value in a list five times.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
from typing import Any

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def build(env: DynamicRealtimeEnvironment) -> Any:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    data = []

    def test_process(k):
        val = 0
        for i in range(5):
            val += env.now
            data.append(val)
            L(k, "add", {"event_type": "added", "val": val})
            yield env.timeout(1)
            L(k, "pause", {"event_type": "waited"})

    p = env.process(test_process("a-1"))
    return p
