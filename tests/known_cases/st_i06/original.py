"""ST-I06: Two interrupts reach a process that ends after the first. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_interrupts.py test_multiple_interrupts
    env, log, L = new()

    def child(k):
        try:
            yield env.timeout(1)
            L(k, "child", {"event_type": "finished"})
        except simpy.Interrupt as i:
            L(k, "child", S("interrupted", cause=i.cause))
            L(k, "return_cause", {"cause": i.cause})
            return i.cause

    def parent(k):
        c = env.process(child(k))
        yield env.timeout(0)
        L(k, "parent", {"event_type": "go"})
        c.interrupt(1)
        L(k, "second", {"event_type": "second"})
        c.interrupt(2)
        result = yield c
        assert result == 1
        L(k, "join", {"event_type": "joined", "result": result})

    arrival(env, "p", parent, initial=1)
    return go(env, log)
