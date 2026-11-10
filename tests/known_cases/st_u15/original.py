"""ST-U15: Chained any-of conditions. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_util.py test_any_of_chaining
    env, log, L = new()

    def pem(k):
        condition_a = env.any_of([env.timeout(2, value="a")])
        condition_b = env.any_of([env.timeout(1, value="b")])
        condition_a |= condition_b
        results = yield condition_a
        assert list(results.values()) == ["b"]
        L(
            k,
            "pem",
            {"event_type": "done", "at": env.now, "values": list(results.values())},
        )

    arrival(env, "a", pem, initial=1)
    return go(env, log)
