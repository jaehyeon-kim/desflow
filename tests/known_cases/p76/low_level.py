"""P76: Enter a queue sorted or at the head.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
from simpy.resources.resource import SortedQueue as _SQ
from simpy.resources.store import StoreGet as _SG
import itertools as _it
import itertools
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


class _GStore(simpy.Store):
    GetQueue = _SQ


_SEQ = _it.count()


class _RG(_SG):
    def __init__(self, s, rank, lifo=False):
        self.key = (rank, -next(_SEQ) if lifo else next(_SEQ))
        super().__init__(s)


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    draws = {"arrival.g.attributes.rank": itertools.cycle([3, 2, 1, 0])}
    q = _GStore(env)

    def want(k):
        rank = next(draws["arrival.g.attributes.rank"])
        yield _RG(q, rank, True)
        L(k, "want", {"event_type": "got"})

    def supply(k):
        yield q.put(None)
        L(k, "supply", {"event_type": "supplied"})

    arrival(env, "g", want, gap=0.5, max_events=4, keyfn=None)
    arrival(env, "p", supply, gap=2, max_events=4, first=1)
