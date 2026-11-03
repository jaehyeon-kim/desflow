"""ST-P06: A parent interrupted while joining a child is not resumed by it. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import PROCS, S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_process.py test_interrupted_join
    env, log, L = new()

    def interruptor(k):
        yield env.timeout(1)
        L(k, "interruptor", {"event_type": "interrupt"})
        PROCS["p-" + k.rsplit("-", 1)[1]].interrupt()

    def child(k):
        yield env.timeout(2)
        L(k, "child", {"event_type": "child_done"})

    def parent(k):
        child_proc = env.process(child(k))
        try:
            yield child_proc
            L(k, "parent", {"event_type": "joined"})
        except simpy.Interrupt as i:
            assert env.now == 1 and child_proc.is_alive
            L(k, "parent", S("interrupted", cause=i.cause))
            # We should not get resumed when child terminates.
            yield env.timeout(5)
            assert env.now == 6
            L(k, "after", {"event_type": "after_interrupt"})

    arrival(env, "p", parent, initial=1)
    arrival(env, "i", interruptor, initial=1)
    return go(env, log)
