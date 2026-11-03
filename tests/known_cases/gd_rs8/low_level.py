"""GD-RS8: A producer and two consumers on a store of two.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = 5


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

    def producer(k, env, store):
        for i in range(100):
            yield env.timeout(2)
            yield store.put(f"spam {i}")
            L(k, "produce", {"event_type": "produced"})

    def consumer(k, env, store):
        while True:
            yield env.timeout(1)
            L(k, "consume", {"event_type": "requesting"})
            item = yield store.get()
            L(k, "take", {"event_type": "got", "item": item})

    store = simpy.Store(env, capacity=2)
    arrival(env, "producer", lambda k: producer(k, env, store), initial=size)
    arrival(env, "consumers", lambda k: consumer(k, env, store), initial=2)
