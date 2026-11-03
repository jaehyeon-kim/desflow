"""ST-RT1: Ten steps take a second of wall time. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import WALL, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_rt with factor 0.1: two processes that sleep 0.01 and 0.02 s of wall time per step, run(2)
    import time

    env, log, L = new(rt=0.1)
    olog = []

    def process(k, sleep_time, timeout=1):
        while True:
            time.sleep(sleep_time)
            yield env.timeout(timeout)
            olog.append(env.now)
            L(k, "pem", {"event_type": "tick"})

    sleeps = iter((0.01, 0.02))
    arrival(env, "a", lambda k: process(k, next(sleeps)), initial=2)
    r = go(env, log, 2)
    assert olog == [1, 1]
    assert 2 * 0.1 <= WALL[0]  # check_duration(duration, 2*factor), lower bound
    return r


program.sizes = (2,)  # the test starts two processes with two sleep times
