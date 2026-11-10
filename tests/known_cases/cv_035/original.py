"""CV-035: Run with no end time. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def _ticks(name, task, gap, n, payload):
    env, log, L = new()

    def t(k):
        L(k, task, payload)
        yield env.timeout(0)

    arrival(env, name, t, gap=gap, max_events=n)
    return go(env, log)


def program():
    log, err = _ticks("a", "tick", 1, 3, {"event_type": "tick"})
    assert (
        err is None and [r[0] for r in log] == [1, 2, 3]
    )  # stated: env.run() with no until ends by itself when no event is left, after the third tick
    return log, err
