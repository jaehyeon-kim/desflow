"""DS-A4: An arrival gap computed from the time. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/A4.py run(lambda t: 1 + t / 4, stop=8): the gap is computed from the time
    env, log, L = new()

    def gap(t):
        return 1 + t / 4

    n = [0]

    def src():
        while True:
            yield env.timeout(gap(env.now))
            n[0] += 1
            L(f"a-{n[0]}", "t", {"event_type": "arrived"})

    env.process(src())
    return go(env, log, 8)
