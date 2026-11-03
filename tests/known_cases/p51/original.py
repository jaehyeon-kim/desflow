"""P51: Setup or changeover. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import VARS, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    m = simpy.Resource(env, 1)
    last = ["none"]

    def job(k):
        kind = ["a", "a", "b", "a"][
            (int(k.split("-")[1]) - 1) % 4
        ]  # the entity number in the key
        r = m.request()
        yield r
        yield env.timeout((2 if last[0] != kind else 0) + 3)
        L(k, "job", {"event_type": "done"})
        last[0] = kind
        m.release(r)

    arrival(env, "a", job, gap=1, max_events=8)
    r = go(env, log)
    VARS["last_kind"] = last[0]
    return r


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    (
        "kind: {gen: one_of, choices: [a, b]}",
        "kind: {gen: expr, expr: \"['a', 'a', 'b', 'a'][n % 4]\", names: {n: {gen: sequence}}}",
    )
]
