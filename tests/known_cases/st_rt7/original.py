"""ST-RT7: Run until an event that never triggers, on a paced environment. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_rt.py: run(until=event) with no event scheduled
    env, log, L = new(rt=0.1)

    def pem(k):
        L(k, "pem", {"event_type": "done"})
        yield env.timeout(0)

    arrival(env, "a", pem, initial=1)
    log, err = go(env, log, env.event())
    assert err is not None and err.startswith(
        'RuntimeError: No scheduled events left but "until" event was not triggered:'
    )
    return log, err
