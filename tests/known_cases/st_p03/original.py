"""ST-P03: A waiter waits for a finisher process. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_process.py test_wait_for_proc
    env, log, L = new()

    def finisher(k):
        yield env.timeout(5)
        L(k, "finisher", {"event_type": "finisher_done"})

    def waiter(k):
        proc = env.process(finisher(k))
        yield proc  # Waits until "proc" finishes
        assert env.now == 5
        L(k, "waiter", {"event_type": "waiter_done"})

    arrival(env, "w", waiter, initial=1)
    return go(env, log)
