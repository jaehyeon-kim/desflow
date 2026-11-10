"""ST-V05: An event succeeded before the process waits. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_event.py test_triggered: the event is succeeded before the process starts to wait
    env, log, L = new()
    event = env.event()

    def maker(k):
        L(k, "maker", {"event_type": "made"})
        event.succeed("i was already done")
        yield env.timeout(0)

    def pem(k):
        value = yield event
        L(k, "pem", {"event_type": "got", "value": value})
        return value

    arrival(env, "m", maker, initial=1)
    arrival(env, "w", pem, initial=1)
    return go(env, log)
