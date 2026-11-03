"""P72: A signal that wakes at most a number of waiters.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment

PROCS: dict = {}  # entity key -> its process
UNTIL = 10


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


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    waiting = []

    def waiter(k):
        ev = env.event()
        waiting.append(ev)
        yield ev
        L(k, "waiter", {"event_type": "woke"})

    def fire(k):
        L(k, "fire", {"event_type": "fire"})
        if waiting:
            waiting.pop(0).succeed()
            L(k, "give", {"event_type": "give"})
        yield env.timeout(0)

    arrival(env, "w", waiter, gap=1, max_events=3, first=0)
    arrival(env, "f", fire, gap=2, max_events=2, first=3)
