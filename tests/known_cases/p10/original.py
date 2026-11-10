"""P10: Change a source from a flow. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import VARS, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    g = [
        2
    ]  # audit2/patterns/mine/g_a2.py p10_simpy(gap0=2, svc=4, newgap=3, until=14): each arrival starts a job that serves 4, then sets the gap to 3

    def job(k):
        L(k, "k", {"event_type": "arrived"})
        yield env.timeout(4)
        L(k, "w", {"event_type": "changed"})
        g[0] = 3

    arrival(env, "o", job, gap=lambda: g[0])
    r = go(env, log, 14)
    VARS["P.arrival.o.value"] = g[0]
    return r
