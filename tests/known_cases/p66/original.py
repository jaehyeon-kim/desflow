"""P66: Stop after a number of departures. The plain SimPy program: from the program the gap register ran."""

import simpy
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    # patterns/i_run.py I1: one unit, an order every 1, service 1.5, stop at the 3rd departure
    m = simpy.Resource(env, 1)
    done = env.event()
    cnt = [0]

    def job(k):
        with m.request() as r:
            yield r
            yield env.timeout(1.5)
        cnt[0] += 1
        L(k, "t", {"e": 1})
        if cnt[0] == 3:
            done.succeed()

    arrival(env, "orders", job, gap=1)
    log, err = go(env, log, done)
    assert env.now == 5.5 and [x[0] for x in log] == [2.5, 4.0, 5.5], (
        env.now,
        log,
    )  # I1 stops at the 3rd departure
    return log, err
