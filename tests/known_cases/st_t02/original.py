"""ST-T02: A negative delay fails when the run reaches it. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_timeout.py test_negative_timeout
    env, log, L = new()

    def pem(k):
        yield env.timeout(-1)
        L(k, "pem", {"event_type": "unreachable"})

    arrival(env, "a", pem, initial=1)
    r = go(env, log)
    assert (
        r[1] is not None and r[1].startswith("ValueError") and "Negative delay" in r[1]
    )
    return r
