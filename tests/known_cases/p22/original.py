"""P22: A branch by fixed weights. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def route(k):
        L(k, "route", {"event_type": "route"})
        chosen = "b" if int(env.now) % 4 == 0 else "a"  # one in four takes b
        L(k, chosen, {"event_type": chosen})
        yield env.timeout(0)

    arrival(env, "a", route, gap=1, max_events=8)
    return go(env, log)


# The draw is fixed: the test applies these replacements to model.yaml.
DRAWS = [
    (
        "next: {weights: {a: 3, b: 1}}",
        'next: {when: [{condition: "t % 4 == 0", task: b}], default: a}',
    )
]
