"""CV-072: Wait on several requests. The plain SimPy program: from the design's SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    m = simpy.Resource(env, 1)
    n = simpy.Resource(env, 1)

    def block(k):
        with m.request() as r:
            yield r
            yield env.timeout(5)
        L(k, "block", {"event_type": "blocked"})

    def either(k):
        ra = m.request()
        rb = n.request()
        yield ra | rb
        win = "b" if rb.triggered else "a"
        if win == "b":
            ra.cancel()
        else:
            rb.cancel()
        yield env.timeout(1)
        (rb if win == "b" else ra).resource.release(rb if win == "b" else ra)
        L(k, "either", {"event_type": "served", "winner": win == "b"})

    arrival(env, "blockers", block, initial=1)
    arrival(env, "a", either, gap=1, max_events=2)
    log, err = go(env, log)
    assert (
        all(r[3]["winner"] for r in log if r[2] == "either")
        and len(m.queue) == 0
        and m.count == 0
        and n.count == 0
    )  # stated: one winner (n), the loser request leaves the queue of m, one holder at a time
    return log, err
