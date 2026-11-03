"""EX-7b: One generator that broadcasts to several pipes.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

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

    pipes = [simpy.Store(env) for _ in range(2)]

    def speak(k):
        message = {
            "event_type": "said",
            "sent": env.now,
            "text": "Generator A says hello at %g" % env.now,
        }
        for pipe in pipes:
            pipe.put(message)
        L(k, "speak", message)
        L(k, "send", {"event_type": "sent"})
        yield env.timeout(0)

    def listen(index):

        def run(k):
            while True:
                item = yield pipes[index].get()
                L(
                    k,
                    "listen",
                    {
                        "event_type": "received",
                        "late": item["sent"] < env.now,
                        "text": item["text"],
                    },
                )
                yield env.timeout(6)
                L(k, "busy", {"event_type": "handled"})

        return run

    arrival(env, "generator", speak, gap=8)
    arrival(env, "consumer_a", listen(0), initial=size)
    arrival(env, "consumer_b", listen(1), initial=1)
