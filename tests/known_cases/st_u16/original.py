"""ST-U16: Any of events that have already been processed. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_util.py test_any_of_with_triggered_events
    env, log, L = new()

    def pem(k):
        events = [env.timeout(0, value="spam"), env.timeout(1, value="eggs")]
        yield env.timeout(2)
        L(k, "pem", {"event_type": "opened"})
        values = list((yield env.any_of(events)).values())
        assert values == ["spam", "eggs"]
        L(k, "collect", {"event_type": "done", "at": env.now, "values": values})

    arrival(env, "p", pem, initial=1)
    return go(env, log)
