"""ST-E06: An end time written as text. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_environment.py test_run_until_value
    env, log, L = new()

    def pem(k):
        while True:
            L(k, "pem", {"event_type": "now"})
            yield env.timeout(1)
            L(k, "pem_wait", {"event_type": "tick"})

    arrival(env, "a", pem, initial=1)
    r = go(env, log, "3.141592")
    assert env.now == 3.141592
    return r
