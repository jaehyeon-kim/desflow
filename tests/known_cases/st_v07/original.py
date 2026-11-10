"""ST-V07: Run until an or condition with an event already succeeded. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_event.py test_condition_callback_removal
    env, log, L = new()
    a, b = env.event(), env.event()
    cond = a | b

    def maker(k):
        L(k, "maker", {"event_type": "made"})
        if not a.triggered:
            a.succeed()
        yield env.timeout(0)

    arrival(env, "m", maker, initial=1)
    r = go(env, log, cond)
    # The condition has removed its callback from event b.
    assert not a.callbacks and not b.callbacks
    return r
