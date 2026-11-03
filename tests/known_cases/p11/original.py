"""P11: A timeout from a named distribution. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def t(k):
        yield env.timeout(2.0)
        L(k, "t", {"event_type": "done"})

    arrival(env, "a", t, gap=1, max_events=5)
    return go(env, log)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [("s: {dist: lognormal, mean: 2, std: 0.5}", "s: {dist: constant, value: 2}")]
