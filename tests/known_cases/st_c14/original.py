"""ST-C14: The result of a condition does not change later. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_condition.py test_immutable_results
    env, log, L = new()

    def pem(k):
        timeout = [env.timeout(delay, value=delay) for delay in range(3)]
        condition = timeout[0] | (timeout[1] & timeout[2])
        results = yield condition
        assert results == {timeout[0]: 0}
        L(
            k,
            "pem",
            {"event_type": "first_done", "in_result": [t in results for t in timeout]},
        )
        yield env.timeout(2)
        assert results == {timeout[0]: 0}
        L(
            k,
            "later",
            {"event_type": "later", "in_result": [t in results for t in timeout]},
        )

    arrival(env, "a", pem, initial=1)
    return go(env, log)
