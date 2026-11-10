"""CV-113: A cleanup that always runs. The plain SimPy program: from the design's SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    procs = []

    def a(k):
        bad = env.now == 2
        procs.append(env.active_process)
        try:
            yield env.timeout(2)
            if bad:
                L(k, "body", S("failed", reason="body failed"))
                L(k, "cleanup", {"event_type": "cleanup"})
                return
            L(k, "body", {"event_type": "body"})
            L(k, "cleanup", {"event_type": "cleanup"})
        except simpy.Interrupt as i:
            L(k, "body", S("interrupted", cause=i.cause))
            L(k, "cleanup", {"event_type": "cleanup"})

    def hit(k):
        L(k, "hit", {"event_type": "hit"})
        for p in list(procs):
            if p.is_alive and p is not env.active_process:
                p.interrupt("x")
        yield env.timeout(0)

    arrival(env, "a", a, gap=1, max_events=3)
    arrival(env, "i", hit, gap=2.5, max_events=1)
    return go(env, log)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [("bad: {gen: boolean}", 'bad: {gen: expr, expr: "t == 2"}')]
