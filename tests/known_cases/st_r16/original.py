"""ST-R16: A container with a putter and a getter. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_container
    env, log, L = new()
    olog = []

    def putter(k, buf):
        yield env.timeout(1)
        L(k, "wait_one", {"event_type": "begin"})
        while True:
            yield buf.put(2)
            olog.append(("p", env.now))
            L(k, "put", {"event_type": "p"})
            yield env.timeout(1)
            L(k, "pause", {"event_type": "pause"})

    def getter(k, buf):
        yield buf.get(1)
        olog.append(("g", env.now))
        L(k, "get_one", {"event_type": "g"})
        yield env.timeout(1)
        L(k, "pause_get", {"event_type": "pause"})
        yield buf.get(1)
        olog.append(("g", env.now))
        L(k, "get_two", {"event_type": "g"})

    buf = simpy.Container(env, init=0, capacity=2)
    arrival(env, "putter", lambda k: putter(k, buf), initial=1)
    arrival(env, "getter", lambda k: getter(k, buf), initial=1)
    r = go(env, log, 5)
    assert olog == [("p", 1), ("g", 1), ("g", 2), ("p", 2)]
    return r
