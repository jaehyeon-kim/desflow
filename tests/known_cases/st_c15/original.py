"""ST-C15: Two processes wait on the same and condition. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_condition.py test_shared_and_condition and test_shared_or_condition (ST-C15, C16): the timeouts and conditions are made before the processes
    env, log, L = new()
    ta = [env.timeout(d, value=d) for d in range(3)]
    c1 = ta[0] & ta[1]
    c2 = c1 & ta[2]
    tb = [env.timeout(d, value=d) for d in range(3)]
    d1 = tb[0] | tb[1]
    d2 = d1 | tb[2]

    def waiter(task, cond, ts, expect):
        def p(k):
            results = yield cond
            assert results == {ts[i]: i for i in expect}
            L(
                k,
                task,
                {
                    "event_type": "done",
                    "at": env.now,
                    "values": [results[t] for t in ts if t in results],
                    "in_result": [t in results for t in ts],
                },
            )

        return p

    arrival(env, "p1", waiter("and_1", c1, ta[:2], (0, 1)), initial=1)
    arrival(env, "p2", waiter("and_2", c2, ta, (0, 1, 2)), initial=1)
    arrival(env, "p3", waiter("or_1", d1, tb[:2], (0,)), initial=1)
    arrival(env, "p4", waiter("or_2", d2, tb, (0,)), initial=1)
    return go(env, log)
