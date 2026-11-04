"""CV-133: An interrupt during a queued request. The plain SimPy program: from the design's SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import PROCS, S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    m = simpy.Resource(env, 1)

    def hold(k):
        with m.request() as r:
            yield r
            yield env.timeout(3)
        L(k, "hold", {"event_type": "hold"})

    def left(k):
        r = m.request()
        try:
            yield r
            yield env.timeout(1)
            m.release(r)
            L(k, "left", {"event_type": "left_served"})
        except simpy.Interrupt as i:
            r.cancel() if not r.triggered else m.release(r)
            L(k, "left", S("interrupted", cause=i.cause))

    def kept(k):
        r = m.request()
        while True:
            try:
                yield r
                break
            except simpy.Interrupt as i:
                L(k, "kept", S("interrupted", cause=i.cause))
        yield env.timeout(1)
        m.release(r)
        L(k, "kept", {"event_type": "kept_served"})

    def again(k):
        r = m.request()
        while True:
            try:
                yield r
                break
            except simpy.Interrupt as i:
                L(k, "again", S("interrupted", cause=i.cause))
                r.cancel()
                r = m.request()
        yield env.timeout(1)
        m.release(r)
        L(k, "again", {"event_type": "again_served"})

    def poke(k):
        L(k, "poke", {"event_type": "poke"})
        PROCS["a-1"].interrupt("x")
        L(k, "poke_kept", {"event_type": "poke_kept"})
        PROCS["b-1"].interrupt("x")
        L(k, "poke_again", {"event_type": "poke_again"})
        PROCS["c-1"].interrupt("x")
        yield env.timeout(0)

    arrival(
        env, "h", hold, initial=1
    )  # the holder first, the three others at 0.5, so the start order does not decide who holds
    for nm, fn in (("a", left), ("b", kept), ("c", again)):
        arrival(env, nm, fn, gap=0.5, max_events=1)
    arrival(env, "i", poke, gap=1, max_events=1)
    return go(env, log)
