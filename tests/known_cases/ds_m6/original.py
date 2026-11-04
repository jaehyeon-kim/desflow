"""DS-M6: The flow name of a process. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/simpy-gap/review-r3/python-checks/cancel_and_name/sp.py worker(): the process reads env.active_process.name at 0, 1 and 2; the process function is named `worker`
    env, log, L = new()

    def worker(k):
        L(k, "worker", {"event_type": "one", "name": env.active_process.name})
        yield env.timeout(1)
        L(k, "two", {"event_type": "two", "name": env.active_process.name})
        yield env.timeout(1)
        L(k, "three", {"event_type": "three", "name": env.active_process.name})

    arrival(env, "a", worker, initial=1)
    log, err = go(env, log)
    assert (
        [(r[0], {q[3]["name"] for q in log}) for r in log]
        == [(0, {"worker"}), (1, {"worker"}), (2, {"worker"})]
    )  # stated (cancel_and_name): the process is named worker and keeps the name at 0, 1 and 2
    return log, err
