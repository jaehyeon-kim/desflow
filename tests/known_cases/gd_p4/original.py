"""GD-P4: Wait again for the same request after an interrupt. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import PROCS, S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    m = simpy.Resource(env, 1)

    def block(k):
        with m.request() as r:
            yield r
            yield env.timeout(3)
        L(k, "block", {"event_type": "blocked_done"})

    def victim(k):
        r = m.request()
        while True:
            try:
                yield r
                break  # the same request is yielded again after the Interrupt
            except simpy.Interrupt as i:
                L(k, "victim", S("interrupted", cause=i.cause))
        left = 1  # the same wait is resumed after an Interrupt in the service too
        while left > 0:
            t0 = env.now
            try:
                yield env.timeout(left)
                left = 0
            except simpy.Interrupt as i:
                left -= env.now - t0
                L(k, "victim", S("interrupted", cause=i.cause))
        m.release(r)
        L(k, "victim", {"event_type": "victim_done"})

    def poker(k):
        L(k, "poker", {"event_type": "poke"})
        for q, p in list(PROCS.items()):
            if q.startswith("victim-") and p.is_alive:
                p.interrupt("x")
        yield env.timeout(0)

    arrival(env, "blocker", block, initial=1)
    arrival(env, "victim", victim, initial=1)
    arrival(env, "poke", poker, gap=1, max_events=1)
    return go(env, log)
