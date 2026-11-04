"""DS-V9: Arguments for a child flow. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/V9.py: the parent starts child a with k * 3 (k is 2), sets k to 100, starts child b with k * 3, logs that it goes on first, then waits for a and b; each child waits 1 and returns x + 1.
    env, log, L = new()

    def child(k, name, x):
        L(k, "child", {"event_type": "starts", "x": x})
        yield env.timeout(1)
        L(k, "child_end", {"event_type": "ended", "v": x + 1})
        return x + 1

    def parent(k):
        kk = 2
        a = env.process(child(k, "a", kk * 3))
        kk = 100
        b = env.process(child(k, "b", kk * 3))
        L(k, "start_b", {"event_type": "parent_goes_on_first"})
        va = yield a
        vb = yield b
        L(k, "wait_b", {"event_type": "results", "va": va, "vb": vb})

    arrival(env, "a", parent, gap=4, max_events=1, first=0)
    return go(env, log)
