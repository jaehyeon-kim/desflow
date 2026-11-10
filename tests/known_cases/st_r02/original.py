"""ST-R02: Two users share one unit with the with form. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_resource_context_manager
    env, log, L = new()
    res = simpy.Resource(env, capacity=1)
    olog = []

    def pem(k):
        name = chr(97 + len(olog) + len(res.users) + len(res.queue))
        with res.request() as request:
            yield request
            yield env.timeout(1)
        L(k, "pem", {"event_type": "done"})
        olog.append((name, env.now))

    arrival(env, "users", pem, initial=2)
    r = go(env, log)
    assert olog == [("a", 1), ("b", 2)]
    return r
