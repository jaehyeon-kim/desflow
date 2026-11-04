"""DS-A4: An arrival gap computed from the time.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment

PROCS: dict = {}  # entity key -> its process
UNTIL = 8


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    def gap(t):
        return 1 + t / 4

    n = [0]

    def src():
        while True:
            yield env.timeout(gap(env.now))
            n[0] += 1
            L(f"a-{n[0]}", "t", {"event_type": "arrived"})

    env.process(src())
