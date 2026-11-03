"""ST-U11: Chained all-of conditions with intermediate results. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_util.py test_all_of_chaining_intermediate_results
    from simpy.events import ConditionValue

    env, log, L = new()

    def pem(k):
        condition_a = env.all_of([env.timeout(i, value=i) for i in range(2)])
        condition_b = env.all_of([env.timeout(i, value=i) for i in range(2)])
        yield env.timeout(0)
        condition = condition_a & condition_b
        result = ConditionValue()
        condition._populate_value(result)
        assert list(result.values()) == [0, 0]
        results = yield condition
        assert list(results.values()) == [0, 1, 0, 1]
        L(
            k,
            "pem",
            {"event_type": "done", "at": env.now, "values": list(results.values())},
        )

    arrival(env, "p", pem, initial=1)
    return go(env, log)
