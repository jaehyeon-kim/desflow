"""P04: A fixed number of arrivals with drawn gaps. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def t(k):
        L(k, "t", {"event_type": "arrived"})
        yield env.timeout(0)

    arrival(env, "a", t, gap=lambda: 1.0, max_events=3)
    log, err = go(env, log, 100)
    assert len(log) == 3, log
    return (log, err)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [("dist: exponential, rate: 1", "dist: constant, value: 1")]
