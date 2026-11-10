"""ST-U19: An all-of expansion is one flat condition. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def pem(k):
        # test_all_of_expansion
        timeouts = [env.timeout(d, d) for d in [3, 2, 1]]
        cond = yield env.all_of(timeouts)
        a, b, c = cond.values()
        assert a == 3
        assert b == 2
        assert c == 1
        L(k, "pem", {"event_type": "done", "count": len(cond.todict())})

    arrival(env, "a", pem, initial=1)
    return go(env, log)
