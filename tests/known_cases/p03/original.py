"""P03: First arrival at 0, then drawn gaps. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def t(k):
        L(k, "t", {"event_type": "arrived"})
        yield env.timeout(0)

    arrival(env, "a", t, gap=5, first=0)
    log, err = go(
        env, log, 12
    )  # a_arrivals.py A1: an arrival at 0, then every 5, run(until=12)
    assert [x[0] for x in log] == [0, 5, 10], (
        log
    )  # A1 prints out = [(0,1),(5,2),(10,3)]
    return log, err
