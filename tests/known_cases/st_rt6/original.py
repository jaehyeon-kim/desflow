"""ST-RT6: Sync the clock after a long computation. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, flush, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_rt_sync: factor 0.05, the process sleeps 0.01 s per step, 0.06 s of work, then sync() and run(3)
    import time

    env, log, L = new(rt=0.05)

    def process(k, sleep_time=0.01, timeout=1):
        while True:
            time.sleep(sleep_time)
            yield env.timeout(timeout)
            L(k, "pem", {"event_type": "tick"})

    arrival(env, "a", process, initial=1)
    flush()  # the arrival is registered before the work, as env.process is in the test
    time.sleep(0.06)  # Simulate massive workload :-)
    env.sync()
    return go(env, log, 3)
