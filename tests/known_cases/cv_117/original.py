"""CV-117: A nested generator called recursively. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/Y6.py recursion(2): f(k) waits 1, calls f(k-1) as a process, logs (now, k, v) and returns v + k; f(0) returns 0
    env, log, L = new()

    def f(key, k):
        L(key, "f", {"event_type": "call", "k": k})
        if k == 0:
            L(key, "base", {"event_type": "base", "ret": 0})
            return 0
        yield env.timeout(1)
        L(key, "step", {"event_type": "tick", "k": k})
        v = yield env.process(f(key, k - 1))
        L(key, "down", {"event_type": "level", "k": k, "v": v, "ret": v + k})
        return v + k

    def g(key):
        total = yield env.process(f(key, 2))
        L(key, "g", {"event_type": "total", "total": total})

    arrival(env, "a", g, initial=1)
    return go(env, log)
