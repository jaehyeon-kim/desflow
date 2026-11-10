"""ST-C02: And with an event that never triggers stays blocked. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_condition.py test_operator_and_blocked
    env, log, L = new()

    def pem(k):
        timeout = env.timeout(1)
        event = env.event()
        yield env.timeout(1)
        L(k, "pem", {"event_type": "waited"})
        condition = timeout & event
        assert not condition.triggered

    arrival(env, "a", pem, initial=1)
    return go(env, log)
