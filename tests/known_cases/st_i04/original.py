"""ST-I04: A child interrupted at the instant it is created. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_interrupts.py test_init_interrupt
    env, log, L = new()

    def child(k):
        try:
            yield env.timeout(10)
            L(k, "child", {"event_type": "should_not_finish"})
        except simpy.Interrupt as i:
            assert env.now == 0
            L(k, "child", S("interrupted", cause=i.cause))
            L(k, "caught", {"event_type": "interrupted"})

    def root(k):
        L(k, "root", {"event_type": "root"})
        child_proc = env.process(child("child-" + k.rsplit("-", 1)[1]))
        L(k, "stop_child", {"event_type": "stop_child"})
        child_proc.interrupt()
        yield env.timeout(1)
        L(k, "root_wait", {"event_type": "root_done"})

    arrival(env, "root", root, initial=1)
    return go(env, log)
