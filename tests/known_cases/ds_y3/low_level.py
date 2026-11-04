"""DS-Y3: A wait on a computed number of events.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

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

    pipes = [simpy.Store(env) for _ in range(3)]

    def child(i):
        yield env.timeout(i + 1)
        return {"event_type": "worker", "v": i * 10}

    def fan(k):
        children = []
        for i in range(3):
            children.append(env.process(child(i)))

        def logged(i, process):
            value = yield process
            L(k, "worker", value)

        for i, process in enumerate(children):
            env.process(logged(i, process))
        results = yield env.all_of(children)
        L(
            k,
            "fan",
            {"event_type": "all_done", "values": [results[c] for c in children]},
        )
        yield env.any_of([env.timeout(d) for d in [4, 1, 2]])
        L(k, "first", {"event_type": "first_done"})
        yield env.all_of([p.put(5) for p in pipes])
        L(k, "broadcast", {"event_type": "sent"})

    arrival(env, "a", fan, initial=size)
