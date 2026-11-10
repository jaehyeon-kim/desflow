"""GD-E4: Start the clock at a time other than 0. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new(100)

    def a(k):
        yield env.timeout(1)
        L(k, "only", {"v": 42})

    arrival(env, "a", a, initial=1)
    log, err = go(env, log)
    assert env.now == 101 and log[0][0] == 101, log  # now starts at initial_time 100
    return log, err
