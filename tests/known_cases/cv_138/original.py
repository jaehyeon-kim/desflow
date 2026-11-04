"""CV-138: Random integers, choices, samples and shuffles. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def draw(k):
        n = 3
        c = "b"
        s = ["a", "c"]
        L(k, "draw", {"event_type": "draw", "n": n, "c": c, "s": s})
        yield env.timeout(0)

    arrival(env, "a", draw, initial=1)
    log, err = go(env, log)
    p = log[0][3]
    assert (
        1 <= p["n"] <= 6
        and p["c"] in ("a", "b", "c")
        and (len(p["s"]) == 2)
        and (len(set(p["s"])) == 2)
        and (set(p["s"]) <= {"a", "b", "c", "d"})
    )
    return (log, err)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    ("n: {gen: integer, bounds: [1, 6]}", "n: 3"),
    ("c: {gen: one_of, choices: [a, b, c]}", "c: b"),
    ("s: {gen: pick, choices: [a, b, c, d], count: 2}", "s: [a, c]"),
]
