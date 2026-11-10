"""GD-R2: A step slower than real time fails only in strict mode. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def _rt(factor, strict):
    env = simpy.rt.RealtimeEnvironment(0, factor=factor, strict=strict)
    log = []

    def L(k, t, p):
        log.append((round(env.now, 6), k, t, p))

    return env, log, L


def program():
    # real-time-simulations.rst:64-88: slow_proc sleeps 0.02 s, factor 0.01, strict raises RuntimeError
    import time

    env, log, L = _rt(0.01, True)

    def one(k):
        time.sleep(0.02)  # Heavy computation :-)
        yield env.timeout(1)
        L(k, "one", {"event_type": "done"})

    arrival(env, "a", one, initial=1)
    log, err = go(env, log, 2)
    assert log == [] and err and err.startswith("RuntimeError: Simulation too slow"), (
        log,
        err,
    )
    return log, err
