"""P23: A probability from an attribute or the state. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def pick(k):
        L(k, "pick", {"event_type": "pick"})
        n = "fast" if int(env.now) % 5 < 4 else "slow"
        L(k, n, {"event_type": n})
        yield env.timeout(0)

    arrival(env, "a", pick, gap=1, max_events=6)
    return go(env, log)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    (
        'next:\n      weights: {fast: "attr.w", slow: "1 - attr.w"}',
        'next: {when: [{condition: "t % 5 < 4", task: fast}], default: slow}',
    )
]
