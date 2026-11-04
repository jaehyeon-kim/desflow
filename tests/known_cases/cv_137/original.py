"""CV-137: The same seed gives the same run. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    None

    def t(k):
        L(k, "t", {"event_type": "arrived"})
        yield env.timeout(0)

    arrival(env, "a", t, gap=lambda: 1.0, max_events=5)
    return go(env, log)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [("dist: exponential, rate: 1,", "dist: constant, value: 1,")]
