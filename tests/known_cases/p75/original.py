"""P75: Read the remaining duration of a component. The plain SimPy program: from the program the gap register ran."""

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
            L(k, "left", {"event_type": "left", "remaining": 10 - (env.now - t0)})

    def hit(k):
        L(k, "hit", {"event_type": "hit"})
        P["w"].interrupt("x")
        yield env.timeout(0)

    P["w"] = env.process(work("a-1"))
    arrival(env, "i", hit, gap=4, max_events=1)
    log, err = go(env, log)
    assert [x[3]["remaining"] for x in log if x[2] == "left"] == [6], (
        log
    )  # work 10, interrupted at 4: 10 - (4 - 0) = 6 remains
    return log, err
