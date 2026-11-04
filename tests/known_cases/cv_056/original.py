"""CV-056: Decide on the cause, then resume. The plain SimPy program: from the design's SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    P = {}

    def work(k):
        t0 = env.now
        try:
            yield env.timeout(10)
            L(k, "work", {"event_type": "done"})
        except simpy.Interrupt as i:
            L(k, "work", S("interrupted", cause=i.cause))
            left = 10 - (env.now - t0)
            L(k, "decide", {"event_type": "decide"})
            if i.cause == "resume":
                yield env.timeout(left)
                L(k, "finish", {"event_type": "finished"})
            else:
                L(k, "stop", {"event_type": "stopped"})

    def hit(k):
        L(k, "hit", {"event_type": "hit"})
        P["w"].interrupt("resume")
        yield env.timeout(0)

    P["w"] = env.process(work("w-1"))
    arrival(env, "i", hit, gap=3, max_events=1)
    return go(env, log)
