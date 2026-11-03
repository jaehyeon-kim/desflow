"""P71: Event times of 0.00001 and 0.00002 stay different. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def t(k):
        yield env.timeout(0.00001)
        L(k, "t", {"event_type": "first", "at": env.now})
        yield env.timeout(0.00001)
        L(k, "u", {"event_type": "second", "at": env.now})

    arrival(env, "a", t, initial=1)
    log, err = go(env, log)
    assert [x[3]["at"] for x in log] == [0.00001, 0.00002], (
        log
    )  # two timeouts of 0.00001 are 0.00001 and 0.00002 apart from 0
    return log, err
