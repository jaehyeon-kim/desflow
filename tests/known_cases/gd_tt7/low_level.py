"""GD-TT7: A run with no end time stops when no event is left.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment

PROCS: dict = {}  # entity key -> its process
UNTIL = 10


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    tlog = []

    def pem(env):
        while env.now < 2:
            tlog.append(env.now)
            L("a-1", "log_now", {"event_type": "logged"})
            yield env.timeout(1)
            L("a-1", "wait_one", {"event_type": "waited"})

    env.process(pem(env))
