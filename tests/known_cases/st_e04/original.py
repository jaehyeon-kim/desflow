"""ST-E04: Run until an event that never triggers. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_environment.py test_run_with_untriggered_event
    env, log, L = new()

    def pem(k):
        L(k, "pem", {"event_type": "done"})
        yield env.timeout(0)

    arrival(env, "a", pem, initial=1)
    log, err = go(env, log, env.event())
    assert err is not None and err.startswith(
        'RuntimeError: No scheduled events left but "until" event was not triggered:'
    )
    return log, err
