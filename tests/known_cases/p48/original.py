"""P48: Periodic shifts. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # register review-r1/patterns/r8_shift.py "P48 simpy": PriorityResource of capacity 1; a shift process waits 10, then asks at priority -1 and holds 3, in a loop; jobs every 1 hold 1 at priority 0; until 35
    env, log, L = new()
    m = simpy.PriorityResource(env, 1)

    def shift(k):
        while True:
            L(k, "offduty", S("started"))
            yield env.timeout(10)
            L(k, "offduty", {"event_type": "on_duty_ends"})
            L(k, "brk", S("queued"))
            with m.request(priority=-1) as r:
                yield r
                L(k, "brk", S("started"))
                yield env.timeout(3)
            L(k, "brk", {"event_type": "break"})

    def job(k):
        L(k, "job", S("queued"))
        with m.request() as r:
            yield r
            L(k, "job", S("started"))
            yield env.timeout(1)
        L(k, "job", {"event_type": "job"})

    arrival(env, "shift", shift, initial=1)
    arrival(env, "jobs", job, gap=1)
    return go(env, log, 35)


program.grants = True
