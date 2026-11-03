"""ST-P02: Read what a process waits for, then interrupt it. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import MODE, PROCS, S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_process.py test_target: the test steps the environment until the process waits on the timeout, checks proc.target and interrupts. Here a look process does that at time 0.
    env, log, L = new()
    event = env.timeout(5)

    def pem(k):
        try:
            yield event
            L(k, "waiter", {"event_type": "waited"})
        except simpy.Interrupt as i:
            L(k, "waiter", S("interrupted", cause=i.cause))

    def look(k):
        L(k, "look", {"event_type": "look"})
        proc = PROCS.get("w-1")
        if not MODE["rev"]:
            assert (
                proc is not None and proc.target is event
            )  # the test steps until the process waits on the event; in the reversed creation order the look runs first
        if proc is not None and proc.target is event:
            proc.interrupt()
        yield env.timeout(0)

    arrival(env, "w", pem, initial=1)
    arrival(env, "look", look, initial=1)
    return go(env, log)
