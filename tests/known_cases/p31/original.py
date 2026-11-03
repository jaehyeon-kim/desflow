"""P31: Hold across several steps and release one of two. The plain SimPy program: from the program the gap register ran."""

import simpy

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    lathe = simpy.Resource(env, 1)
    press = simpy.Resource(env, 1)

    def job(k):
        on_lathe = lathe.request()
        yield on_lathe
        yield env.timeout(1)
        L(k, "take_lathe", {"event_type": "lathe"})
        on_press = press.request()
        yield on_press
        yield env.timeout(2)
        L(k, "take_press", {"event_type": "press"})
        lathe.release(on_lathe)  # the lathe goes back, the press is kept
        yield env.timeout(1)
        L(k, "finish", {"event_type": "done"})
        press.release(on_press)

    arrival(env, "a", job, initial=2)
    return go(env, log)
