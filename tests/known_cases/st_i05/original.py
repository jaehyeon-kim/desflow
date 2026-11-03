"""ST-I05: Interrupt a process that has ended. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import VARS, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_interrupts.py test_interrupt_terminated_process: the interrupt of a process that has ended raises RuntimeError, which the test catches
    env, log, L = new()
    VARS["interrupt_raised"] = False

    def child(k):
        yield env.timeout(1)
        L("child-1", "child", {"event_type": "child_done"})

    def parent(k):
        L(k, "parent", {"event_type": "parent"})
        child_proc = env.process(child(k))
        yield env.timeout(2)
        L(k, "later", {"event_type": "late"})
        try:
            child_proc.interrupt()
        except RuntimeError as e:
            assert "has terminated and cannot be interrupted" in e.args[0]
            VARS["interrupt_raised"] = True
        yield env.timeout(1)

    arrival(env, "parent", parent, initial=1)
    return go(env, log)
