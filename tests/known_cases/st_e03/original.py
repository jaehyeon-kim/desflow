"""ST-E03: Run until an event that has already been processed. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, flush, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_environment.py test_run_with_processed_event
    env, log, L = new()
    timeout = env.timeout(1, value="spam")

    def pem(k):
        yield timeout
        L(k, "pem", {"event_type": "done"})

    arrival(env, "a", pem, initial=1)
    flush()
    err = None
    try:
        assert env.run(until=timeout) == "spam"
        assert env.now == 1
        # timeout has been processed, calling run again will return its value again.
        assert env.run(until=timeout) == "spam"
        assert env.now == 1
    except BaseException as e:
        err = f"{type(e).__name__}: {str(e)[:120]}"
    return log, err
