"""GD-E2: Run until a process ends and return its value.

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

    def my_proc(k):
        yield env.timeout(1)
        L(k, "my_proc", {"text": "Monty Python’s Flying Circus"})
        return "Monty Python’s Flying Circus"

    procs = [env.process(my_proc(f"a-{i + 1}")) for i in range(size)]
    return env.all_of(procs)
