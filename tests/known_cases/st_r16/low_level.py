"""ST-R16: A container with a putter and a getter.

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

    olog = []

    def putter(k, buf):
        yield env.timeout(1)
        L(k, "wait_one", {"event_type": "begin"})
        while True:
            yield buf.put(2)
            olog.append(("p", env.now))
            L(k, "put", {"event_type": "p"})
            yield env.timeout(1)
            L(k, "pause", {"event_type": "pause"})

    def getter(k, buf):
        yield buf.get(1)
        olog.append(("g", env.now))
        L(k, "get_one", {"event_type": "g"})
        yield env.timeout(1)
        L(k, "pause_get", {"event_type": "pause"})
        yield buf.get(1)
        olog.append(("g", env.now))
        L(k, "get_two", {"event_type": "g"})

    buf = simpy.Container(env, init=0, capacity=2)
    arrival(env, "putter", lambda k: putter(k, buf), initial=size)
    arrival(env, "getter", lambda k: getter(k, buf), initial=1)
