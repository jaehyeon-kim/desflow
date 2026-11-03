"""GD-TT4: Interrupted during service, keeping the unit. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, go, new, start

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    res = simpy.Resource(env, 1)

    def worker(env):
        with res.request() as r:
            yield r
            end = env.now + 2
            while env.now < end:
                try:
                    yield env.timeout(end - env.now)
                except simpy.Interrupt as i:
                    L("a-1", "user", S("interrupted", cause=i.cause))
                    assert res.count == 1
        L("a-1", "user", {"event_type": "user_done"})

    def other(env):
        yield env.timeout(0.5)
        with res.request() as r:
            yield r
            yield env.timeout(1)
        L("b-1", "waiting", {"event_type": "waiting_done"})

    H = {}
    start(env, lambda: H.update(a=env.process(worker(env))))
    start(env, lambda: env.process(other(env)))

    def intr(env):
        yield env.timeout(1)
        L("i-1", "poke", {"event_type": "poke"})
        H["a"].interrupt()

    start(env, lambda: env.process(intr(env)))
    return go(env, log)
