"""DS-V10: Routing weights from an expression. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    import random

    env, log, L = new()

    class R(random.Random):
        def random(self):
            return 0.0 if env.now < 6 else 0.99

    r = R()

    def pick(weights):

        def f(k):
            L(k, "pick", {"event_type": "pick"})
            n = r.choices(["fast", "slow"], weights=weights)[0]
            L(k, n, {"event_type": n})
            yield env.timeout(0)

        return f

    arrival(env, "a", pick([1, 0]), gap=1, max_events=4, first=0)
    arrival(env, "b", pick([0.5, 0.5]), gap=1, max_events=4, first=4)
    return go(env, log)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    (
        'next: {weights: {fast: "attr.w", slow: "1 - attr.w"}}',
        'next: {weights: {fast: "1 if t < 6 else 0", slow: "0 if t < 6 else 1"}}',
    )
]
