"""ST-V08: Run until a nested condition with an event already succeeded. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_event.py test_condition_nested_callback_removal
    env, log, L = new()
    a, b, c = env.event(), env.event(), env.event()
    b_and_c = b & c
    a_or_b_and_c = a | b_and_c

    def maker(k):
        L(k, "maker", {"event_type": "made"})
        if not a.triggered:
            a.succeed()
        yield env.timeout(0)

    arrival(env, "m", maker, initial=1)
    r = go(env, log, a_or_b_and_c)
    # Callbacks from nested conditions are also removed.
    assert (
        not a.callbacks
        and not b.callbacks
        and not c.callbacks
        and not a_or_b_and_c.callbacks
    )
    for cb in b_and_c.callbacks:
        assert cb.__name__ != "_check"  # b_and_c may have a _build_value callback
    return r
