"""ST-C05: Or nested in and. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_condition.py test_operator_nested_or
    env, log, L = new()

    def pem(k):
        timeout = [env.timeout(delay, value=delay) for delay in range(3)]
        results = yield (timeout[0] | timeout[1]) & timeout[2]
        assert results == {timeout[0]: 0, timeout[1]: 1, timeout[2]: 2}
        assert env.now == 2
        L(
            k,
            "pem",
            {
                "event_type": "done",
                "at": env.now,
                "values": [results[t] for t in timeout],
                "in_result": [t in results for t in timeout],
            },
        )

    arrival(env, "a", pem, initial=1)
    return go(env, log)
