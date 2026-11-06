"""P20: Branch by an attribute with fixed weights. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def route(k):
        vip = (int(k.split("-")[1]) - 1) % 3 == 0  # the entity number in the key
        L(k, "route", {"event_type": "route"})
        if vip:
            L(k, "fast", {"event_type": "fast"})
        else:
            L(k, "weighted", {"event_type": "weighted"})
            chosen = "a" if int(env.now) % 2 == 0 else "b"
            L(k, chosen, {"event_type": chosen})
        yield env.timeout(0)

    arrival(env, "a", route, gap=1, max_events=6)
    return go(env, log)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    (
        "vip: {gen: boolean, p: 0.3}",
        'vip: {gen: expr, expr: "n % 3 == 0", names: {n: {gen: sequence}}}',
    ),
    (
        "next:\n      weights: {a: 3, b: 1}",
        'next: {when: [{condition: "t % 2 == 0", task: a}], default: b}',
    ),
]
