"""CV-093: A last in, first out store.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy
import simpy.rt

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


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    class LifoStore(simpy.Store):
        def _do_get(self, event):
            if self.items:
                event.succeed(self.items.pop())

    s = LifoStore(env)
    n = [0]

    def put(k):
        v = n[0]
        yield s.put(v)
        L(k, "put_it", {"event_type": "put"})
        n[0] = v + 1

    def get(k):
        it = yield s.get()
        L(k, "get_it", {"event_type": "got", "item": it})

    arrival(env, "p", put, gap=1, max_events=4)
    arrival(env, "g", get, gap=6, max_events=4)
