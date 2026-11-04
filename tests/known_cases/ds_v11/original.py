"""DS-V11: A wait group with a predicate of its own. The plain SimPy program: from the design's SimPy program."""

import simpy
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/V11.py first(): a = timeout 1, b = timeout 3, a Condition whose predicate is count == 1 and now < 5; the process goes on when the predicate holds (at 1).
    env, log, L = new()

    def gate(k):
        a = env.timeout(1, "a")
        b = env.timeout(3, "b")
        yield simpy.events.Condition(env, lambda evs, c: c == 1 and env.now < 5, [a, b])
        L(k, "gate", {"event_type": "opened", "at": env.now})

    arrival(env, "a", gate, gap=1, max_events=1, first=0)
    return go(env, log)
