"""GD-S3: Interrupt a process and read the cause. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    P = {}

    def v(k):
        try:
            yield env.timeout(10)
            L(k, "victim", {"event_type": "done"})
        except simpy.Interrupt as i:
            L(k, "victim", S("interrupted", cause=i.cause))
            L(k, "caught", {"event_type": "interrupted", "cause": i.cause})

    def h(k):
        L(k, "hit", {"event_type": "hit"})
        P["v"].interrupt("Spam")
        yield env.timeout(0)

    P["v"] = env.process(v("v-1"))
    arrival(env, "i", h, gap=1, max_events=1)
    return go(env, log)
