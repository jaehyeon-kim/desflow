"""DS-Y7: Names for another process, an event state and a queue's entries. The plain SimPy program: from the design's SimPy program."""

import simpy
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import PROCS, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/Y7.py: alive() (is_alive), states() (triggered and processed), peek() (started at 100 so no other event is pending) and queues() (users, queue and get_queue at 2).
    env, log, L = new()
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
    return go(env, log)
