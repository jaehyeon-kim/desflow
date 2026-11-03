"""GD-TT5: An event and an interrupt arrive together. The plain SimPy program: from the SimPy program."""

import simpy
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import PROCS, S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def _hit(prefix, cause):
    for k, p in list(PROCS.items()):
        if k.startswith(prefix) and p.is_alive:
            p.interrupt(cause)


def program():
    env, log, L = new()
    sig = env.event()

    def fox(k):
        while True:
            try:
                yield sig
                break
            except simpy.Interrupt as i:
                L(k, "fox", S("interrupted", cause=i.cause))
        L(k, "fox", {"event_type": "coup_completed"})

    def plan(k):
        L(k, "master_plan", {"event_type": "plan"})
        sig.succeed()
        _hit("fox-", None)
        yield env.timeout(0)  # the blueprint's arrival source waits the 1

    arrival(env, "fox", fox, initial=1)
    arrival(env, "plan", plan, gap=1, max_events=1)
    log, err = go(env, log, 5)
    # test_concurrent_interrupts_and_events asserts: ['coup interrupted at 1', 'coup completed at 1']
    assert {
        (t, p.get("status") or p.get("event_type"))
        for t, _, tk, p in log
        if tk == "fox"
    } == {(1, "interrupted"), (1, "coup_completed")}, log
    return log, err
