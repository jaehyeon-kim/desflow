"""ST-P01: Read whether processes are alive. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import PROCS, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_process.py test_get_state
    env, log, L = new()

    def pem_a(k):
        yield env.timeout(3)
        L(k, "pem_a", {"event_type": "a_done"})

    def pem_b(k):
        yield env.timeout(1)
        a = PROCS["a-1"]
        assert a.is_alive
        L(k, "pem_b", {"event_type": "b_first", "a_alive": a.is_alive})
        yield env.timeout(3)
        assert not a.is_alive
        L(k, "pem_b2", {"event_type": "b_done", "a_alive": a.is_alive})

    arrival(env, "a", pem_a, initial=1)
    arrival(env, "b", pem_b, initial=1)
    return go(env, log)
