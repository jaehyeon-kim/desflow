"""ST-RT2: A paced run that is called twice. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import WALL, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_rt with factor 0.1, two processes, run(2) and then run(4)
    env, log, L = new(rt=0.1)
    olog = []

    def process(k):
        while True:
            yield env.timeout(1)
            olog.append(env.now)
            L(k, "pem", {"event_type": "tick"})

    arrival(env, "a", process, initial=2)
    go(env, log, 2)
    result = go(env, log, 4)
    assert olog == [1, 1, 2, 2, 3, 3], olog
    # The paced clock runs from the start of the environment, and so does WALL, so
    # after the second call it holds the wall time of all four units
    assert 4 * 0.1 <= WALL[0]
    return result


program.sizes = (2,)  # the test starts two processes
