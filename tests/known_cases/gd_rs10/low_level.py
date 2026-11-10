"""GD-RS10: A priority store with text priorities.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def arrival(
    env, name, make, initial=0, gap=None, max_events=None, first=None, keyfn=None
):
    """Starts `initial` entities now, in order, then one every `gap`, the first
    `first` after the start. `make(key)` returns the process of the entity `key`."""
    count = [0]

    def key():
        count[0] += 1
        return f"{name}-{count[0]}" if keyfn is None else keyfn()

    def begin(label):
        PROCS[label] = env.process(make(label))

    for _ in range(initial):
        begin(key())
    if gap is not None:

        def source():
            made = 0
            if first is not None and first > 0:
                yield env.timeout(first)
            while max_events is None or made < max_events:
                if not (first is not None and made == 0):
                    yield env.timeout(gap() if callable(gap) else gap)
                made += 1
                begin(key())

        env.process(source())


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    issues = simpy.PriorityStore(env)

    def inspector(k, env, issues):
        for issue in [
            simpy.PriorityItem("P2", "#0000"),
            simpy.PriorityItem("P0", "#0001"),
            simpy.PriorityItem("P3", "#0002"),
            simpy.PriorityItem("P1", "#0003"),
        ]:
            yield env.timeout(1)
            L(
                k,
                "tick",
                {"event_type": "log", "priority": issue.priority, "item": issue.item},
            )
            yield issues.put(issue)
            L(k, "file", {"event_type": "filed"})

    def maintainer(k, env, issues):
        while True:
            yield env.timeout(3)
            L(k, "wait3", {"event_type": "waited"})
            issue = yield issues.get()
            L(
                k,
                "take",
                {
                    "event_type": "repair",
                    "priority": issue.priority,
                    "item": issue.item,
                },
            )

    arrival(env, "inspector", lambda k: inspector(k, env, issues), initial=size)
    arrival(env, "maintainer", lambda k: maintainer(k, env, issues), initial=1)
