"""EX-7a: One generator and one consumer joined by a store.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment, Sampler
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
    sampler = Sampler(seed=42)
    gaps = sampler.generator("arrivals.generator.gap")
    busy = sampler.generator("tasks.busy.service_time")

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    def message_generator(name, env, out_pipe):
        i = 0
        while True:
            yield env.timeout(int(gaps.integers(6, 11)))
            i += 1
            msg = (env.now, f"{name} says hello at {env.now:g}")
            out_pipe.put(msg)
            L(f"generator-{i}", "speak", {"event_type": "said"})

    def message_consumer(k):
        while True:
            msg = yield pipe.get()
            L(
                k,
                "listen",
                {"event_type": "received", "late": msg[0] < env.now, "text": msg[1]},
            )
            yield env.timeout(int(busy.integers(4, 9)))
            L(k, "busy", {"event_type": "handled"})

    pipe = simpy.Store(env)
    env.process(message_generator("Generator A", env, pipe))
    arrival(env, "consumer", message_consumer, initial=size)
