"""GD-TT3: Interrupted while queued, keeping the same request. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import MODE, PROCS, S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def pem(k):
        with res.request() as req:
            yield req
            yield env.timeout(1)
        L(k, "hold", {"event_type": "hold_done"})

    def victim(k):
        evt = res.request()
        try:
            yield evt
            raise AssertionError("Should not have gotten the resource.")
        except simpy.Interrupt as i:
            L(k, "victim", S("interrupted", cause=i.cause))
            yield evt
            res.release(evt)
            if not MODE["on"]:
                assert env.now == 1
            L(k, "victim", {"event_type": "victim_done"})

    def interruptor(k):
        L(k, "poke", {"event_type": "poke"})
        for q, p in list(PROCS.items()):
            if q.startswith("b-") and p.is_alive:
                p.interrupt()
        return 0
        yield

    res = simpy.Resource(env, 1)
    arrival(env, "a", pem, initial=1)
    arrival(env, "b", victim, initial=1)
    arrival(env, "i", interruptor, initial=1)
    return go(env, log)
