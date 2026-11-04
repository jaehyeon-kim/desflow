"""CV-002: Read whether a signal has fired. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    sig = env.event()

    def f(k):
        L(k, "fire_it", {"event_type": "fired"})
        if not sig.triggered:
            sig.succeed()
        yield env.timeout(0)

    def leave(k):
        L(k, "look", {"event_type": "look", "triggered": sig.triggered})
        yield env.timeout(0)

    arrival(env, "f", f, gap=1, max_events=1)
    arrival(env, "l", leave, gap=2, max_events=1)
    log, err = go(env, log)
    assert [p["triggered"] for _, _, t, p in log if t == "look"] == [True] and [
        r[0] for r in log if r[2] == "fire_it"
    ] == [1]  # stated: triggered is true after the fire
    return log, err
