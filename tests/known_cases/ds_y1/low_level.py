"""DS-Y1: Resources, stores and signals in a computed number.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

PROCS: dict = {}  # entity key -> its process
UNTIL = 20


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


def build(env: DynamicRealtimeEnvironment, size: int = 4) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    desks = [simpy.Resource(env, capacity=1) for _ in range(3)]
    q = simpy.Store(env)
    replies = {}

    def event(i):
        return replies.setdefault(i, env.event())

    def job(k):
        i = _number(k) - 1
        with desks[i % 3].request() as request:
            yield request
            yield env.timeout(2)
        L(k, "job", {"event_type": "got", "i": i})

    def client(i):

        def run(k):
            yield q.put(i)
            L(k, "client", {"event_type": "put", "i": i})
            if i == 1:
                yield env.timeout(5)
            L(k, "waits", {"event_type": "waits", "i": i, "at": env.now})
            value = yield event(i)
            L(k, "hear", {"event_type": "reply", "i": i, "v": value, "at": env.now})

        return run

    def server(k):
        while True:
            item = yield q.get()
            yield env.timeout(1)
            L(
                k,
                "server_fire",
                {"event_type": "server_fired", "i": item, "at": env.now},
            )
            if not event(item).triggered:
                event(item).succeed(item * 10)

    arrival(env, "jobs", job, initial=size)
    arrival(env, "c0", client(0), gap=1, max_events=1, first=0)
    arrival(env, "c1", client(1), gap=1, max_events=1, first=0)
    arrival(env, "srv", server, gap=1, max_events=1, first=0)
