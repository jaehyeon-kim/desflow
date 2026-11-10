"""P56: Clone and join all. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # patterns/g_flow.py G1: join waits for process(br(1)) & process(br(3)); one order at 0
    env, log, L = new()

    def join(k):
        def bx():
            yield env.timeout(1)
            L(k, "bx", {"x": 1})

        def by():
            yield env.timeout(3)
            L(k, "by", {"y": 3})

        yield env.process(bx()) & env.process(by())
        L(k, "join", {"joined": 1})

    arrival(env, "orders", join, initial=1)
    log, err = go(env, log, 10)
    assert {x[0] for x in log if x[2] == "join"} == {3}, log  # G1 prints (3, [1, 3])
    return log, err
