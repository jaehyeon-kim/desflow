"""P38: Balking at a long queue. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # register audit2/patterns/mine/g_d2.py P38, condition form: balk when 2 or more wait, service 1.5, entities at one instant
    env, log, L = new()
    m = simpy.Resource(env, 1)

    def job(k):
        if len(m.queue) >= 2:
            L(k, "job", S("balked"))
            return
        with m.request() as r:
            yield r
            yield env.timeout(1.5)
            L(k, "job", {"event_type": "served"})

    arrival(env, "a", job, initial=3)
    log, err = go(env, log)
    served = sorted(
        x[0] for x in log if x[3].get("event_type") == "served"
    )  # g_d2.py: one in service and 2 waiting, later ones balk; service 1.5 one after the other
    assert served == [1.5, 3.0, 4.5][: len(served)] and len(served) == min(
        len(log), 3
    ), log
    return log, err
