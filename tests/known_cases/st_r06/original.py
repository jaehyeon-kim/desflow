"""ST-R06: A process that asks again does not pass the queue. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_resource_immediate_requests
    env, log, L = new()

    def child(env, res, k):
        result = []
        for _ in range(3):
            with res.request() as req:
                yield req
                result.append(env.now)
                yield env.timeout(1)
                L(k, "round", {"event_type": "acquired"})
        return result

    def parent(env):
        res = simpy.Resource(env, 1)
        child_a = env.process(child(env, res, "children-1"))
        child_b = env.process(child(env, res, "children-2"))
        a_acquire_times = yield child_a
        b_acquire_times = yield child_b
        assert a_acquire_times == [0, 2, 4]
        assert b_acquire_times == [1, 3, 5]

    env.process(parent(env))
    return go(env, log)
