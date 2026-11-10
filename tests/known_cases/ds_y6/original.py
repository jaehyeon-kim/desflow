"""DS-Y6: A local variable that two children change at once. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def count(gap):
        L("a-1", "count", {"event_type": "begin"})
        i = 0
        while True:
            yield env.timeout(gap)
            L("a-1", "step", {"event_type": "step", "n": i + 1})
            i += 1
            if not i < 2:
                break

    def parent():
        yield env.process(count(1)) & env.process(count(1.5))
        L("a-1", "parent", {"event_type": "parent"})

    def src():
        yield env.timeout(0)
        env.process(parent())

    env.process(parent())
    return go(env, log)
