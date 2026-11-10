"""GD-TT1: Interrupted while joining a child, then join it again. The plain SimPy program: from the SimPy program."""

import simpy
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, go, new, start

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def child():
        yield env.timeout(2)
        L("p-1", "child", {"event_type": "child_done"})

    def parent():
        child_proc = env.process(child())
        try:
            yield child_proc
            raise AssertionError("Did not receive an interrupt.")
        except simpy.Interrupt as i:
            assert env.now == 1 and child_proc.is_alive
            L("p-1", "parent", S("interrupted", cause=i.cause))
            yield child_proc
            assert env.now == 2
        L("p-1", "parent", {"event_type": "joined"})

    H = {}
    start(env, lambda: H.update(p=env.process(parent())))

    def interruptor():
        yield env.timeout(1)
        L("i-1", "interruptor", {"event_type": "interrupt"})
        H["p"].interrupt()

    start(env, lambda: env.process(interruptor()))
    return go(env, log)
