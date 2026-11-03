"""ST-R05: A queued victim interrupted releases its request. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import PROCS, S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_resource_release_after_interrupt
    env, log, L = new()
    res = simpy.Resource(env, 1)

    def blocker(k):
        with res.request() as req:
            yield req
            yield env.timeout(1)
            L(k, "blocker", {"event_type": "blocker_done"})

    def victim(k):
        evt = res.request()
        try:
            yield evt
            L(k, "victim", {"event_type": "victim_done"})  # pytest.fail in the test
        except simpy.Interrupt as i:
            L(k, "victim", S("interrupted", cause=i.cause))
            res.release(evt)
            assert env.now == 0

    def interruptor(k):
        L(k, "poker", {"event_type": "poke"})
        PROCS["victim-1"].interrupt()
        return 0
        yield

    arrival(env, "blocker", blocker, initial=1)
    arrival(env, "victim", victim, initial=1)
    arrival(env, "poke", interruptor, initial=1)
    return go(env, log)
