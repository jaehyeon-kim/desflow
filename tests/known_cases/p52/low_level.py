"""P52: Batch and separate with a timeout.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

PROCS: dict = {}  # entity key -> its process
UNTIL = 30


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


def _batcher(env, L, size, timeout, rule, task):
    """The batcher of B3.py: members put themselves and an event in a store; a batcher
    takes the first, then more until `size`, the rule or the timeout, and then lets them
    go in the order they came. Returns the member process."""
    st = simpy.Store(env)

    def member(k):
        ev = env.event()
        yield st.put((k, ev))
        yield ev
        L(k, task, {"event_type": "packed"})

    def batcher():
        while True:
            first = yield st.get()
            batch = [first]
            deadline = env.timeout(timeout)
            while len(batch) < size and not (rule and rule(batch)):
                g = st.get()
                yield g | deadline
                if g.triggered:
                    batch.append(g.value)
                if deadline.processed:
                    if not g.triggered:
                        g.cancel()
                    break
            for _, ev in batch:
                ev.succeed()

    env.process(batcher())
    return member


def _members(env, member, names, times, size=1):
    for name, at in zip(names.split(), times):
        if at == 0:
            arrival(env, name, member, initial=size)
        else:
            arrival(env, name, member, gap=1, max_events=1, first=at)


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    plain = _batcher(env, L, 3, 2, None, "pack")
    _members(env, plain, "m0 m1 m2 m3 m4", (0, 1, 5, 5.5, 5.8), size)
