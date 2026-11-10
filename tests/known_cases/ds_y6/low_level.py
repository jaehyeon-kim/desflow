"""DS-Y6: A local variable that two children change at once.

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

    def count(gap):
        L("a-1", "count", {"event_type": "begin"})
        i = 0
        while True:
            yield env.timeout(gap)
            L("a-1", "step", {"event_type": "step", "n": i + 1})
            i += 1
            if not i < 2:
                break

    def parent():
        yield (env.process(count(1)) & env.process(count(1.5)))
        L("a-1", "parent", {"event_type": "parent"})

    def src():
        yield env.timeout(0)
        env.process(parent())

    env.process(parent())
