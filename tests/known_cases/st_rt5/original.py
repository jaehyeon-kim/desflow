"""ST-RT5: An end time below the clock is refused. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_rt_illegal_until: RealtimeEnvironment() with its default factor, then run(-1)
    env, log, L = new(rt=1.0)

    def pem(k):
        L(k, "pem", {"event_type": "tick"})
        yield env.timeout(0)

    arrival(env, "a", pem, initial=1)
    r = go(env, log, -1)
    assert r[1] is not None and r[1].startswith(
        "ValueError: until (-1) must be greater than the current simulation time"
    )
    return r
