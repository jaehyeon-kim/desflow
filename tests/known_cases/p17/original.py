"""P17: Set a computed attribute. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def t(k):
        yield env.timeout(1)
        L(k, "t", {"event_type": "set"})
        L(k, "u", {"event_type": "read", "n": 1, "t0": env.now})

    arrival(env, "a", t, initial=1)
    log, err = go(env, log)
    assert log[1][3]["n"] == 1 and log[1][3]["t0"] == 1, (
        log
    )  # B3: n+1 and the time are stored as numbers, not strings
    return log, err
