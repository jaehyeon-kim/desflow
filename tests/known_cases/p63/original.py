"""P63: Broadcast a signal. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import PROCS, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # patterns/h_signals.py H1 (YAML broadcast form): two waiters at 0, a clerk at 2 fires the signal once; until 10
    env, log, L = new()
    wake = env.event()

    def waitc(k):
        yield wake
        L(k, "waitc", {"woken": 1})

    def clerk(k):
        L(k, "clerk", {})
        wake.succeed()
        yield env.timeout(0)

    arrival(env, "c", waitc, initial=2)
    arrival(env, "k", clerk, gap=2, max_events=1)
    log, err = go(env, log, 10)
    assert {x[0] for x in log if x[2] == "waitc"} == {2} and len(
        [x for x in log if x[2] == "waitc"]
    ) == sum(1 for q in PROCS if q.startswith("c-")), (
        log
    )  # H1 broadcast: every waiter wakes at 2
    return log, err
