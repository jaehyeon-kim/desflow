"""CV-130: A generator function picked by a computed name. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    cnt = {"proc_a": 0, "proc_b": 0}

    def proc(name, d):

        def p():
            cnt[name] += 1
            k = f"{name}-{cnt[name]}"
            yield env.timeout(d)
            L(k, name, {"event_type": name})

        return p

    def a(k):
        w = ["b", "a"][int(env.now) % 2]
        L(k, "pick", {"event_type": "pick"})
        L(k, "run_" + w, {"event_type": "run_" + w})
        env.process(proc("proc_" + w, 1 if w == "a" else 2)())
        yield env.timeout(0)

    arrival(env, "a", a, gap=1, max_events=4)
    return go(env, log)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    (
        "which: {gen: one_of, choices: [a, b]}",
        "which: {gen: expr, expr: \"['b', 'a'][int(t) % 2]\"}",
    )
]
