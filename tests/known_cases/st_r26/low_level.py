"""ST-R26: A later get still matches after a mismatch.

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

    s = simpy.FilterStore(env, 2)

    def getter(k):
        eggs = s.get(lambda i: i == "eggs")
        spam = s.get(lambda i: i == "spam")
        yield (spam | eggs)
        L(k, "getter", {"event_type": "first", "spam_first": spam.triggered})
        yield eggs
        L(k, "second", {"event_type": "eggs_at_one"})

    def putter(k):
        yield s.put("spam")
        L(k, "putter", {"event_type": "put_spam"})
        yield env.timeout(1)
        L(k, "later", {"event_type": "pause"})
        yield s.put("eggs")
        L(k, "put_eggs", {"event_type": "put_eggs"})

    arrival(env, "g", getter, initial=size)
    arrival(env, "p", putter, initial=1)
