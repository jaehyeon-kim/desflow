"""ST-RT4: A step slower than real time does not fail when not strict. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import WALL, arrival, go

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def _rt(factor, strict):
    env = simpy.rt.RealtimeEnvironment(0, factor=factor, strict=strict)
    log = []

    def L(k, t, p):
        log.append((round(env.now, 6), k, t, p))

    return env, log, L


def program():
    # test_rt_slow_sim_no_error: factor 0.05, strict False, the process sleeps 0.1 s per step, run(2) takes 0.2 s
    import time

    env, log, L = _rt(0.05, False)
    olog = []

    def pem(k, sleep_time=0.1, timeout=1):
        while True:
            time.sleep(sleep_time)
            yield env.timeout(timeout)
            olog.append(env.now)
            L(k, "pem", {"event_type": "tick"})

    arrival(env, "a", pem, initial=1)
    r = go(env, log, 2)
    assert 2 * 0.1 <= WALL[0] < 2 * 0.1 + 0.02 and olog == [1]
    return r
