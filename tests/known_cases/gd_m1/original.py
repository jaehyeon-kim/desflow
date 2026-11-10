"""GD-M1: Collect a value in a list five times. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    data = []

    def test_process(k):
        val = 0
        for i in range(5):
            val += env.now
            data.append(val)
            L(k, "add", {"event_type": "added", "val": val})
            yield env.timeout(1)
            L(k, "pause", {"event_type": "waited"})

    p = env.process(test_process("a-1"))
    r = go(env, log, p)
    assert data == [0, 1, 3, 6, 10]
    return r
