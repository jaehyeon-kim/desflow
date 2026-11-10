"""DS-Y7: Names for another process, an event state and a queue's entries.

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


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    ev = env.event()
    r = simpy.PriorityResource(env)
    s = simpy.Container(env, capacity=5, init=0)

    def w(k):
        yield env.timeout(2)

    def watch(k):
        for i in range(3):
            L(
                k,
                "watch" if i == 0 else "watch%d" % (i + 1),
                {"event_type": "alive", "alive": PROCS["c-1"].is_alive},
            )
            yield env.timeout(1)

    def states(k):
        L(
            k,
            "states",
            {
                "event_type": "before",
                "triggered": ev.triggered,
                "processed": ev.processed,
            },
        )
        ev.succeed(1)
        L(
            k,
            "states_fired",
            {
                "event_type": "fired",
                "triggered": ev.triggered,
                "processed": ev.processed,
            },
        )
        yield env.timeout(0)
        L(
            k,
            "states_after",
            {
                "event_type": "after",
                "triggered": ev.triggered,
                "processed": ev.processed,
            },
        )

    def j(p, at):

        def f(k):
            yield env.timeout(at)
            with r.request(priority=p) as q:
                yield q
                yield env.timeout(5)

        return f

    def g(a):

        def f(k):
            yield s.get(a)

        return f

    def look(k):
        L(
            k,
            "look",
            {
                "event_type": "look",
                "users": [[u.priority, u.usage_since] for u in r.users],
                "queue": [[u.priority, u.time] for u in r.queue],
                "get_queue": [e.amount for e in s.get_queue],
            },
        )
        yield env.timeout(0)

    def peeker(k):
        env.timeout(3)
        L(k, "peeker", {"event_type": "peek", "next_event": env.peek(), "at": env.now})
        yield env.timeout(1)
        L(k, "peeker2", {"event_type": "peek", "next_event": env.peek(), "at": env.now})

    arrival(env, "c", w, gap=1, max_events=1, first=0)
    arrival(env, "p", watch, gap=1, max_events=1, first=0)
    arrival(env, "a", states, gap=1, max_events=1, first=0)
    arrival(env, "j1", j(1, 0), gap=1, max_events=1, first=0)
    arrival(env, "j2", j(3, 0.5), gap=1, max_events=1, first=0)
    arrival(env, "j3", j(2, 1), gap=1, max_events=1, first=0)
    arrival(env, "g1", g(2), gap=1, max_events=1, first=0)
    arrival(env, "g2", g(3), gap=1, max_events=1, first=0)
    arrival(env, "lk", look, gap=1, max_events=1, first=2)
    arrival(env, "pk", peeker, gap=1, max_events=1, first=100)
