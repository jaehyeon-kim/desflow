"""ST-U10: Chained all-of conditions. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_all_of_chaining
    env, log, L = new()

    def pem(k):
        condition_a = env.all_of([env.timeout(i, value=i) for i in range(2)])
        condition_b = env.all_of([env.timeout(i, value=i) for i in range(2)])
        condition_a &= condition_b
        results = yield condition_a
        assert list(results.values()) == [0, 1, 0, 1]
        L(k, "pem", {"event_type": "done", "at": env.now})

    arrival(env, "p", pem, initial=1)
    return go(env, log)
