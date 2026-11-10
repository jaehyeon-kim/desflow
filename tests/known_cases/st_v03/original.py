"""ST-V03: A timeout keeps its value after the run. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_event.py test_value
    env, log, L = new()
    event = env.timeout(0, "I am the value")

    def pem(k):
        v = yield event
        L(k, "pem", {"event_type": "got", "value": v})

    arrival(env, "a", pem, initial=1)
    r = go(env, log)
    assert event.value == "I am the value"
    return r
