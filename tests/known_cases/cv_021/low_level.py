"""CV-021: The events of a condition and at least.

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

    def waits(task, condition):
        def p(k):
            evs = [env.timeout(d, value=d * 10) for d in [3, 1, 2]]
            res = yield condition(evs)
            v = [res[e] if e in res else None for e in evs]
            L(
                k,
                task,
                {"event_type": "done", "at": env.now, "a": v[0], "b": v[1], "c": v[2]},
            )

        return p

    def at_least(evs):
        return simpy.events.Condition(env, lambda e, c: c >= 2, evs)

    arrival(env, "a", waits("two_of_three", at_least), initial=size)
    arrival(env, "b", waits("all_three", env.all_of), initial=1)
    arrival(env, "c", waits("any_of_three", env.any_of), initial=1)
