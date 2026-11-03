"""P69: Leave with a probability. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def t(k):
        L(k, "t", {"e": 1})
        n = "gone" if int(env.now) % 2 == 0 else "stay"
        L(k, n, {"left": 1} if n == "gone" else {"stay": 1})
        yield env.timeout(0)

    arrival(env, "orders", t, gap=1)
    return go(env, log, 5)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    (
        "next: {weights: {gone: 1, stay: 1}}",
        'next: {when: [{condition: "t % 2 == 0", task: gone}], default: stay}',
    )
]
