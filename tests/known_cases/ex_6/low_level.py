"""EX-6: Messages that reach a store after a fixed delay.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = 100


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

    n = [0]

    class Cable:
        def __init__(self, env, delay):
            self.env = env
            self.delay = delay
            self.store = simpy.Store(env)

        def latency(self, k, value):
            yield self.env.timeout(self.delay)
            done = self.store.put(value)
            done.callbacks.append(
                lambda e: L(k, "latency", {"event_type": "delivered"})
            )

        def put(self, k, value):
            n[0] += 1
            self.env.process(self.latency(f"latency-{n[0]}", value))

        def get(self):
            return self.store.get()

    cable = Cable(env, 10)

    def send(k):
        L(k, "send", {"event_type": "sent"})
        cable.put(k, f"Sender sent this at {env.now:g}")
        yield env.timeout(0)

    def receive(k):
        while True:
            m = yield cable.get()
            L(k, "receive", {"event_type": "received", "msg": m})

    arrival(env, "sender", send, gap=5)
    arrival(env, "receiver", receive, initial=size)
