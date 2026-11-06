"""P02: An attribute drawn for each entity. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def t(k):
        x = [0.5, 1.5, 0.25][
            int(k.split("-")[1]) % 3 - 1
        ]  # the entity number in the key
        L(k, "t", {"event_type": "seen", "x": x})
        yield env.timeout(0)

    arrival(env, "a", t, gap=lambda: 1.0)
    return go(env, log, 20)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    ("dist: exponential\n    rate: 1", "dist: constant\n    value: 1"),
    (
        "x: {gen: sample, dist: exponential, rate: 2}",
        'x: {gen: expr, expr: "[0.5, 1.5, 0.25][n % 3]", names: {n: {gen: sequence}}}',
    ),
]
