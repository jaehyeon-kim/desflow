"""CV-128: A resource picked by a computed name. The plain SimPy program: from the design's SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    res = {"left": simpy.Resource(env, 1), "right": simpy.Resource(env, 1)}

    def a(k):
        side = ["right", "left"][int(env.now) % 2]
        L(k, "pick", {"event_type": "pick"})
        with res[side].request() as r:
            yield r
            yield env.timeout(2)
            L(k, "at_" + side, {"event_type": "served_" + side})

    arrival(env, "a", a, gap=1, max_events=4)
    return go(env, log)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    (
        "side: {gen: one_of, choices: [left, right]}",
        "side: {gen: expr, expr: \"['right', 'left'][int(t) % 2]\"}",
    )
]
