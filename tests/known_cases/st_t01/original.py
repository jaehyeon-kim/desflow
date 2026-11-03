"""ST-T01: A loop that logs the time every second. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_timeout.py test_discrete_time_steps
    env, log, L = new()
    olog = []

    def pem(k):
        while True:
            olog.append(env.now)
            L(k, "pem", {"event_type": "now", "now": env.now})
            yield env.timeout(delay=1)
            L(k, "step", {"event_type": "step"})

    arrival(env, "a", pem, initial=1)
    r = go(env, log, 3)
    assert olog == [0, 1, 2]
    return r
