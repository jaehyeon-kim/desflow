"""GD-B1: A timeout that carries a value. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # simpy_basics.rst: example() creates simpy.events.Timeout(env, delay=1, value=42) and prints now and the value
    env, log, L = new()

    def example(k):
        event = simpy.events.Timeout(env, delay=1, value=42)
        value = yield event
        L(
            k, "example", {"event_type": "printed", "value": value}
        )  # 'now=%d, value=%d' % (env.now, value); the record time is now

    arrival(env, "a", example, initial=1)
    log, err = go(env, log)
    assert [(t, p["value"]) for t, _, _, p in log] == [(1, 42)], (
        log
    )  # the guide prints now=1, value=42
    return log, err
