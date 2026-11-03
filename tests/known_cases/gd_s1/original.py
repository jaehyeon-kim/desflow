"""GD-S1: The SimPy 2 verbs written for SimPy 4. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
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
        yield env.timeout(1) | ev[0]
        L(k, "either", {"event_type": "first_of_two"})
        yield env.timeout(1) & ev[0]
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

    arrival(env, "a", a, initial=1)
    arrival(env, "b", b, gap=2, max_events=1)
    return go(env, log)
