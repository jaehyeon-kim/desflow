"""P54: Assembly from two stores.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = 4.2


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

    sa = simpy.Store(env)
    sb = simpy.Store(env)

    def pa(k):
        yield sa.put(1)
        L(k, "pa", {"part": "a"})

    def pb(k):
        yield sb.put(1)
        L(k, "pb", {"part": "b"})

    def assemble(k):
        yield (sa.get() & sb.get())
        L(k, "assemble", {"done": 1})

    arrival(env, "a", pa, gap=1)
    arrival(env, "b", pb, gap=2)
    arrival(env, "asm", assemble, gap=0.5)
