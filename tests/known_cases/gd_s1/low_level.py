"""GD-S1: The SimPy 2 verbs written for SimPy 4.

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


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    r = simpy.Resource(env, 1)
    q = simpy.Container(env, 10, 0)
    ev = [env.event()]

    def fire():
        e = ev[0]
        ev[0] = env.event()
        e.succeed()

    def a(k):
        yield env.timeout(1)
        L(k, "hold", {"event_type": "hold"})
        yield ev[0]
        L(k, "passivate", {"event_type": "reactivated"})
        with r.request() as rq:
            yield rq
            yield env.timeout(1)
        L(k, "request", {"event_type": "used"})
        yield q.get(5)
        L(k, "get", {"event_type": "got"})
        yield (env.timeout(1) | ev[0])
        L(k, "either", {"event_type": "first_of_two"})
        yield (env.timeout(1) & ev[0])
        L(k, "both", {"event_type": "both"})
        yield env.all_of([env.timeout(1), env.timeout(2), env.timeout(3)])
        L(k, "all_of", {"event_type": "all_of_three"})
        yield env.any_of([env.timeout(3), env.timeout(1), env.timeout(2)])
        L(k, "any_of", {"event_type": "any_of_three"})

        def calculation(env):
            yield env.timeout(1)
            L(k, "calculation", {"event_type": "calculated"})

        yield env.process(calculation(env))
        L(k, "process", {"event_type": "process_done"})

    def b(k):
        yield q.put(5)
        L(k, "put_it", {"event_type": "put"})
        fire()

    arrival(env, "a", a, initial=size)
    arrival(env, "b", b, gap=2, max_events=1)
