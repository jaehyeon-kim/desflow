"""ST-I07: A child waiting on an event interrupted at time 0. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_interrupts.py test_immediate_interrupt
    env, log, L = new()

    def child(k):
        try:
            yield env.event()
            L(k, "child", {"event_type": "finished"})
        except simpy.Interrupt as i:
            L(k, "child", S("interrupted", cause=i.cause))
            L(k, "caught", {"event_type": "interrupted", "at": env.now})

    def parent(k):
        child_proc = env.process(child("child-" + k.rsplit("-", 1)[1]))
        L(k, "parent", {"event_type": "parent"})
        L(k, "stop", {"event_type": "interrupt"})
        child_proc.interrupt()
        return
        yield

    arrival(env, "parent", parent, initial=1)
    return go(env, log)
