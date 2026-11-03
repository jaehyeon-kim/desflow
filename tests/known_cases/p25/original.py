"""P25: Rework chosen by weights. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def make(k):
        while True:
            yield env.timeout(1)
            L(k, "make", {"event_type": "made"})
            L(k, "check", {"event_type": "checked"})
            if int(env.now) % 3 == 0:  # a check at a time of 3, 6, 9 sends the job back
                continue
            L(k, "pass", {"event_type": "pass"})
            break

    arrival(env, "a", make, gap=2, max_events=5)
    return go(env, log)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    (
        "next: {weights: {make: 1, pass: 4}}",
        'next: {when: [{condition: "t % 3 == 0", task: make}], default: pass}',
    )
]
