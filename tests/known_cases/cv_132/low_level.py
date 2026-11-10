"""CV-132: A process waited for by two waiters.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment

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


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    sig = env.event()

    def w(k):
        yield env.timeout(2)
        L(k, "worker", {"event_type": "worker_done"})
        if not sig.triggered:
            sig.succeed(7)

    def wa(k):
        yield sig
        L(k, "waiter_a", {"event_type": "a_saw", "at": env.now, "v": sig.value})

    def wb(k):
        yield sig
        L(k, "waiter_b", {"event_type": "b_saw_after", "at": env.now, "v": sig.value})

    arrival(env, "w", w, initial=1)
    arrival(env, "a", wa, initial=1)
    arrival(env, "b", wb, gap=5, max_events=1)
