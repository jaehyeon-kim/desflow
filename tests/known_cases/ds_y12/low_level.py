"""DS-Y12: A rank on the waiting gets and puts of a store and a container.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
from simpy.resources.container import ContainerGet as _CGet
from simpy.resources.resource import SortedQueue as _SortedQueue
from simpy.resources.store import StoreGet as _StoreGet
from simpy.resources.store import StorePut as _StorePut
import itertools as _it
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


class _GetRanked(simpy.Store):
    GetQueue = _SortedQueue


class _PutRanked(simpy.Store):
    PutQueue = _SortedQueue


class _RankedContainer(simpy.Container):
    GetQueue = _SortedQueue


_YSEQ = _it.count()


class _RankedGet(_StoreGet):  # research/yaml-only/programs/Y12.py RankedGet
    def __init__(self, store, rank):
        self.key = (rank, store._env.now, next(_YSEQ))
        super().__init__(store)


class _RankedGetC(_CGet):
    def __init__(self, c, amount, rank):
        self.key = (rank, c._env.now, next(_YSEQ))
        super().__init__(c, amount)


class _RankedPut(_StorePut):
    def __init__(self, store, item, rank):
        self.key = (rank, store._env.now, next(_YSEQ))
        super().__init__(store, item)


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    sA = _GetRanked(env)
    sB = _GetRanked(env)
    shelf = _PutRanked(env, capacity=1)
    c = _RankedContainer(env, capacity=10, init=0)

    def getter(s, tag, i, r):

        def f(k):
            v = yield _RankedGet(s, r)
            L(k, "get" + tag, {"event_type": "got", "i": i, "item": v})

        return f

    def putter(s, tag):

        def f(k):
            yield env.timeout(1)
            for number, x in enumerate("abc", 1):
                yield s.put(x)
                L(k, f"put{tag}{number}", {"event_type": "put"})

        return f

    def shelve(name, r):

        def f(k):
            yield _RankedPut(shelf, name, r)
            L(k, "shelve", {"event_type": "in", "name": name})

        return f

    def take(k):
        v = yield shelf.get()
        L(k, "take", {"event_type": "out", "name": v})

    def cget(name, a, r):

        def f(k):
            yield _RankedGetC(c, a, r)
            L(k, "cget", {"event_type": "got", "name": name, "level": c.level})

        return f

    def cput(k):
        yield env.timeout(1)
        yield c.put(2)
        yield env.timeout(1)
        yield c.put(3)

    for n, f in (
        ("ga0", getter(sA, "A", 0, 5)),
        ("ga1", getter(sA, "A", 1, 1)),
        ("ga2", getter(sA, "A", 2, 3)),
        ("pa", putter(sA, "A")),
        ("gb0", getter(sB, "B", 0, 1)),
        ("gb1", getter(sB, "B", 1, 1)),
        ("gb2", getter(sB, "B", 2, 0)),
        ("pb", putter(sB, "B")),
    ):
        arrival(env, n, f, gap=1, max_events=1, first=0)
    arrival(env, "wf", shelve("first", 0), initial=size)
    arrival(env, "wl", shelve("low", 5), initial=1)
    arrival(env, "wh", shelve("high", 1), initial=1)
    arrival(env, "tk", take, gap=1, max_events=3, first=1)
    arrival(env, "cbig", cget("big, rank 0", 5, 0), gap=1, max_events=1, first=0)
    arrival(env, "csmall", cget("small, rank 1", 1, 1), gap=1, max_events=1, first=0)
    arrival(env, "cp", cput, gap=1, max_events=1, first=0)
