"""CV-004: Read whether a signal or a wait branch succeeded. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    sig = env.event()

    def w(k):
        yield sig
        L(k, "wait_x", {"event_type": "ok", "signal_ok": sig.ok, "branch_ok": sig.ok})

    def f(k):
        L(k, "fire_it", {"event_type": "fired"})
        sig.succeed()
        yield env.timeout(0)

    arrival(env, "w", w, initial=1)
    arrival(env, "f", f, gap=1, max_events=1)
    log, err = go(env, log)
    assert (
        sig.ok is True
        and [(p["signal_ok"], p["branch_ok"]) for _, _, t, p in log if t == "wait_x"]
        == [(True, True)]
    )  # stated: Event.ok is true after succeed, read at the signal and at the wait branch
    return log, err
