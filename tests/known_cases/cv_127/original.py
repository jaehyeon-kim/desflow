"""CV-127: A capacity or level computed at the start. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import VARS, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    n = 4
    cap = [1]

    def a(k):
        L(k, "setup", {"event_type": "setup"})
        cap[0] = n * 2
        yield env.timeout(0)

    arrival(env, "setup", a, initial=1)
    r = go(env, log)
    VARS["n"] = n
    VARS["C.resources.m.current_cap"] = cap[
        0
    ]  # the original's stated result is cap == 8; compared with the document's registry
    assert cap[0] == 8
    return r
