"""ST-C17: The values of a condition. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_condition.py test_condition_value, test_result_order, test_nested_result_order (ST-C17, C18, C19), one after the other
    env, log, L = new()

    def pem(k):
        timeouts = [env.timeout(delay, value=delay) for delay in range(3)]
        results = yield env.all_of(timeouts)
        assert list(results) == timeouts and list(results.keys()) == timeouts
        assert list(results.values()) == [0, 1, 2] and list(results.items()) == list(
            zip(timeouts, [0, 1, 2])
        )
        assert timeouts[0] in results and results[timeouts[0]] == 0
        assert results == results.todict()
        L(
            k,
            "value",
            {
                "event_type": "done",
                "at": env.now,
                "values": list(results.values()),
                "has_first": timeouts[0] in results,
                "first": results[timeouts[0]],
            },
        )
        plain = [env.timeout(delay) for delay in range(3)]
        name = {t: f"t{i}" for i, t in enumerate(plain)}
        timeouts = list(reversed(plain))
        results = yield env.all_of(timeouts)
        assert list(results.keys()) == timeouts
        L(
            k,
            "result_order",
            {"event_type": "done", "order": [name[e] for e in results.keys()]},
        )
        timeouts = [env.timeout(delay) for delay in range(3)]
        name = {t: f"t{i}" for i, t in enumerate(timeouts)}
        condition = (timeouts[0] | timeouts[1]) & timeouts[2]
        results = yield condition
        assert list(results.keys()) == timeouts
        L(
            k,
            "nested_result_order",
            {"event_type": "done", "order": [name[e] for e in results.keys()]},
        )

    arrival(env, "a", pem, initial=1)
    return go(env, log)
