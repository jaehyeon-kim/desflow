"""GD-R1: One time unit takes a tenth of a second. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import go

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def _rt(factor, strict):
    env = simpy.rt.RealtimeEnvironment(0, factor=factor, strict=strict)
    log = []

    def L(k, t, p):
        log.append((round(env.now, 6), k, t, p))

    return env, log, L


def program():
    import time

    dur = []

    def example(env, k=None, L=None):
        start = time.perf_counter()
        yield env.timeout(1)
        end = time.perf_counter()
        dur.append(end - start)
        if L:
            L(k, "one", {"event_type": "done"})

    plain = simpy.Environment()
    proc = plain.process(example(plain))
    plain.run(until=proc)
    assert round(dur[0], 2) == 0.0
    env, log, L = _rt(0.1, True)  # strict defaults to True
    proc = env.process(example(env, "a-1", L))
    r = go(env, log, proc)
    assert round(dur[1], 1) == 0.1, dur
    return r
