"""CV-111: Handle a failure of several steps. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def a(k):
        bad = env.now == 2
        try:
            yield env.timeout(1)
            if bad:
                raise RuntimeError("step one failed")
            L(k, "step_one", {"event_type": "one"})
            yield env.timeout(1)
            if bad:
                raise RuntimeError("step two failed")
            L(k, "step_two", {"event_type": "two"})
        except RuntimeError as e:
            L(
                k,
                "step_one" if "one" in str(e) else "step_two",
                S("failed", reason=str(e)),
            )
            L(k, "handle", {"event_type": "handled"})

    arrival(env, "a", a, gap=1, max_events=3)
    return go(env, log)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [("bad: {gen: boolean}", 'bad: {gen: expr, expr: "t == 2"}')]
