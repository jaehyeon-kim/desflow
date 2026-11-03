"""ST-R08: A request or a timeout, whichever comes first. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_resource_with_condition
    env, log, L = new()
    res = simpy.Resource(env, 1)

    def pem(k):
        with res.request() as res_event:
            result = yield res_event | env.timeout(1)
            assert res_event in result
            L(k, "pem", {"event_type": "done", "got_it": res_event in result})

    arrival(env, "a", pem, initial=1)
    return go(env, log)
