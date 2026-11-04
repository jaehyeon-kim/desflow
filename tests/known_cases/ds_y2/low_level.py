"""DS-Y2: A member picked by a computed rule.

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


def _number(key):
    """The number an entity key ends in, from 1."""
    return int(key.rsplit("-", 1)[1])


def build(env: DynamicRealtimeEnvironment, size: int = 2) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    desk = [simpy.Resource(env, 1) for _ in range(3)]
    named = [simpy.Resource(env, 1) for _ in range(3)]
    pair = [simpy.Resource(env, 1) for _ in range(2)]

    def pick(rs):
        return min(range(len(rs)), key=lambda n: len(rs[n].queue) + rs[n].count)

    def shortest(k):
        i = _number(k) - 1
        with desk[pick(desk)].request() as request:
            yield request
            yield env.timeout(1 + i)
        L(k, "shortest", {"event_type": "done", "i": i})

    def by_index(k):
        i = _number(k) - 1
        with named[i % 2].request() as request:
            yield request
            yield env.timeout(1 + i)
        L(k, "by_index", {"event_type": "done", "i": i})

    def same_step(k):
        with pair[pick(pair)].request() as request:
            yield request
            yield env.timeout(1)
        L(k, "same_step", {"event_type": "done"})

    arrival(env, "a", shortest, gap=0.5, max_events=5, first=0)
    arrival(env, "b", by_index, gap=0.5, max_events=5, first=0)
    arrival(env, "d", same_step, initial=size)
