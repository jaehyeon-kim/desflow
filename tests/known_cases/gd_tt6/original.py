"""GD-TT6: An unhandled exception stops the run. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def boom(env):
        yield env.timeout(1)
        L("a-1", "boom", S("failed", reason="boom"))
        raise ValueError("boom")

    env.process(boom(env))
    log, err = go(env, log, 5)
    assert err == "ValueError: boom", err  # the error leaves run(until=5)
    return log, err
