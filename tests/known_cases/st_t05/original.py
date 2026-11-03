"""ST-T05: A child starts after a timeout has been processed. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_timeout.py test_triggered_timeout
    env, log, L = new()

    def child(k, event):
        value = yield event
        return value

    def process(k):
        L(k, "parent", {"event_type": "parent"})
        event = env.timeout(1, "i was already done")
        # Start the child after the timeout has already happened.
        yield env.timeout(2)
        L(k, "later", {"event_type": "later"})
        value = yield env.process(child(k, event))
        assert value == "i was already done"
        L(k, "child", {"event_type": "child", "value": value})

    arrival(env, "p", process, initial=1)
    return go(env, log)
