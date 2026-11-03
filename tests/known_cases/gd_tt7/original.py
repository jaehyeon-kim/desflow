"""GD-TT7: A run with no end time stops when no event is left. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    tlog = []

    def pem(env):
        while env.now < 2:
            tlog.append(env.now)
            L("a-1", "log_now", {"event_type": "logged"})
            yield env.timeout(1)
            L("a-1", "wait_one", {"event_type": "waited"})

    env.process(pem(env))
    r = go(env, log, 10)
    assert tlog == [0, 1]
    return r
