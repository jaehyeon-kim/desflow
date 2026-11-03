"""ST-I10: A process that tries to interrupt itself. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import VARS, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_interrupts.py test_interrupt_self: the self-interrupt raises RuntimeError, which the test catches
    env, log, L = new()
    VARS["interrupt_raised"] = False

    def pem(k):
        try:
            env.active_process.interrupt()
        except RuntimeError:
            VARS["interrupt_raised"] = True
        yield env.timeout(0)
        L(k, "pem", {"event_type": "done"})

    arrival(env, "a", pem, initial=1)
    return go(env, log)
