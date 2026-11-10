"""CV-135: A class with fields and methods. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def a(k):
        bal = 0
        L(k, "deposit", {"event_type": "deposit", "balance": bal + 10})
        bal += 10
        L(k, "withdraw", {"event_type": "withdraw"})
        bal -= 4
        if bal == 6:  # the method a test picks
            L(k, "spend", {"event_type": "spend", "balance": bal})
        else:
            L(k, "save", {"event_type": "save", "balance": bal})
        yield env.timeout(0)

    arrival(env, "a", a, initial=1)
    log, err = go(env, log)
    assert (
        [r[2] for r in log] == ["deposit", "withdraw", "spend"]
        and log[0][3]["balance"] == 10
    )  # stated: the field holds 10 after the deposit method, then the withdraw method runs, and the test picks spend
    return log, err
