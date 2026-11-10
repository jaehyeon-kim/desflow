"""ST-R03: Nine users on three units. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def _seqkeys(start=0):
    c = [start - 1]

    def f():
        c[0] += 1
        return str(c[0])

    return f


def program():
    # test_resource_slots
    env, log, L = new()
    m = simpy.Resource(env, capacity=3)
    olog = []

    def pem(k):
        with m.request() as req:
            yield req
            olog.append((k, env.now))
            yield env.timeout(1)
            L(k, "pem", {"event_type": "granted"})

    arrival(env, "users", pem, initial=9, keyfn=_seqkeys())
    r = go(env, log)
    assert olog == [
        ("0", 0),
        ("1", 0),
        ("2", 0),
        ("3", 1),
        ("4", 1),
        ("5", 1),
        ("6", 2),
        ("7", 2),
        ("8", 2),
    ]
    return r
