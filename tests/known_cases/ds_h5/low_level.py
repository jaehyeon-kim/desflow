"""DS-H5: Tasks that take resources in opposite orders.

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

    r1 = simpy.Resource(env, 1)
    r2 = simpy.Resource(env, 1)

    def job(a, b, names):

        def make(k):
            with a.request() as q:
                yield q
                yield env.timeout(1)
                L(k, names[0], {"event_type": names[3]})
                with b.request() as s:
                    yield s
                    yield env.timeout(1)
                    L(k, names[1], {"event_type": "got_both"})
                L(k, names[2], {"event_type": names[4]})

        return make

    arrival(
        env,
        "first",
        job(r1, r2, ("one_two", "one_two_b", "one_two_c", "got_r1", "free_r1")),
        initial=size,
    )
    arrival(
        env,
        "second",
        job(r2, r1, ("two_one", "two_one_b", "two_one_c", "got_r2", "free_r2")),
        initial=1,
    )
