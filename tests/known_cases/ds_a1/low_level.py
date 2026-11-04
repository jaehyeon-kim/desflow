"""DS-A1: A fixed number of arrivals.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    count = 3

    def t(k):
        L(k, "t", {"event_type": "arrived"})
        yield env.timeout(0)

    def src(name):
        for i in range(count):
            yield env.timeout(2)
            env.process(t(f"{name}-{i + 1}"))

    env.process(src("a"))
