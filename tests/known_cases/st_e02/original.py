"""ST-E02: Run in three segments. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, flush, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_environment.py test_run_resume
    env, log, L = new()
    events = [env.timeout(t) for t in (5, 10, 15)]

    def pem(k, event):
        yield event
        L(k, "pem", {"event_type": "done"})

    for n, e in zip("abc", events):
        arrival(env, n, lambda k, e=e: pem(k, e), initial=1)
    flush()
    err = None
    try:
        assert env.now == 0 and not any(event.processed for event in events)
        env.run(until=10)
        assert (
            env.now == 10
            and all(event.processed for event in events[:1])
            and not any(event.processed for event in events[1:])
        )
        env.run(until=15)
        assert (
            env.now == 15
            and all(event.processed for event in events[:2])
            and not any(event.processed for event in events[2:])
        )
        env.run()
        assert env.now == 15 and all(event.processed for event in events)
    except BaseException as e:
        err = f"{type(e).__name__}: {str(e)[:120]}"
    return log, err
