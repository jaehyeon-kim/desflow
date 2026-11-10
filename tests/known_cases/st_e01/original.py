"""ST-E01: The clock ends at the run time when the queue is empty. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_environment.py test_event_queue_empty
    env, log, L = new()
    olog = []

    def pem(k):
        while env.now < 2:
            olog.append(env.now)
            L(k, "pem", {"event_type": "now", "now": env.now})
            yield env.timeout(1)
            L(k, "pause", {"event_type": "tick"})

    arrival(env, "a", pem, initial=1)
    r = go(env, log, 10)
    assert olog == [0, 1]
    return r
