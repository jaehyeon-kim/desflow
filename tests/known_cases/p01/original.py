"""P01: Fixed starting attributes. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def t(k):
        L(k, "t", {"event_type": "seen", "kind": "standard"})
        yield env.timeout(0)

    arrival(env, "a", t, gap=lambda: 1.0)
    return go(env, log, 20)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [("dist: exponential\n    rate: 1", "dist: constant\n    value: 1")]
