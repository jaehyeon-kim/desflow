"""DS-A1: A fixed number of arrivals. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/A1.py run(3): a source waits 2 and starts t, three times
    env, log, L = new()
    count = 3

    def t(k):
        L(k, "t", {"event_type": "arrived"})
        yield env.timeout(0)

    def src(name):
        for i in range(count):
            yield env.timeout(2)
            env.process(t(f"{name}-{i + 1}"))

    env.process(src("a"))
    log, err = go(env, log)
    assert (
        [r[0] for r in log] == [2, 4, 6] and env.now == 6
    )  # stated (A1 run(3)): arrivals at 2, 4, 6 and the source ends at 6
    return log, err
