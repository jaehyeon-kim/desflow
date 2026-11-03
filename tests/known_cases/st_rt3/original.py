"""ST-RT3: A step slower than real time fails in strict mode. The plain SimPy program: from the SimPy test's program."""

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
    # test_rt_slow_sim_default_behavior: factor 0.05, the process sleeps 0.1 s per step, run(3) raises
    import time

    env, log, L = _rt(0.05, True)
    olog = []

    def pem(k, sleep_time=0.1, timeout=1):
        while True:
            time.sleep(sleep_time)
            yield env.timeout(timeout)
            olog.append(env.now)
            L(k, "pem", {"event_type": "tick"})

    arrival(env, "a", pem, initial=1)
    r = go(env, log, 3)
    assert olog == []
    assert (
        r[1] is not None
        and r[1].startswith("RuntimeError")
        and "Simulation too slow for real time" in r[1]
    )
    return r
