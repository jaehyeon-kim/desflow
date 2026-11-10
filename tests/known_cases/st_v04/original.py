"""ST-V04: Read the value of an event before it triggers. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import VARS, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_event.py test_unavailable_value: reading the value of an untriggered event raises AttributeError, which the test catches
    env, log, L = new()
    VARS["value_raised"] = False
    event = env.event()

    def pem(k):
        value = None
        try:
            value = event.value
        except AttributeError as e:
            assert str(e).endswith("is not yet available")
            VARS["value_raised"] = True
        L(k, "pem", {"event_type": "early", "value": value})
        yield env.timeout(0)

    arrival(env, "a", pem, initial=1)
    return go(env, log)
