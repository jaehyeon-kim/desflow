"""GD-S4: Return the first needle found.

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

    s = simpy.Store(env)
    s.items += [{"needle": 0, "n": 1}, {"needle": 0, "n": 2}, {"needle": 1, "n": 3}]

    def is_needle(item):
        return item["needle"] == 1

    def find_first_needle(k):
        while True:
            item = yield s.get()
            L(k, "find", {"event_type": "looked", "item": item})
            if is_needle(item):
                return item

    def proc(k):
        needle = yield env.process(find_first_needle(k))
        L(k, "found", {"event_type": "found", "needle": needle})

    arrival(env, "a", proc, initial=size)
