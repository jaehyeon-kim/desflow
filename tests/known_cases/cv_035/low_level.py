"""CV-035: Run with no end time.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The run has no end time, so it ends when no event is left.
"""

from desflow import DynamicRealtimeEnvironment

UNTIL = None


def build(env: DynamicRealtimeEnvironment) -> None:
    def tick(key):
        env.publish_event(key, {"event_type": "tick"}, task="tick")
        yield env.timeout(0)

    def source():
        for number in range(1, 4):
            yield env.timeout(1)
            env.process(tick(f"a-{number}"))

    env.process(source())
