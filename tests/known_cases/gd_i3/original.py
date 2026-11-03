"""GD-I3: A driver interrupts a car that catches the interrupt. The plain SimPy program: from the SimPy program."""

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

    def car(k):
        while True:
            try:
                yield env.process(charge(k))
                L(k, "park_and_charge", {"event_type": "charged_and_parked"})
            except simpy.Interrupt as i:
                L(k, "park_and_charge", S("interrupted", cause=i.cause))
            yield env.timeout(2)
            L(k, "drive", {"event_type": "drove"})

    def charge(k):
        yield env.timeout(5)
        L(k, "charge_task", {"event_type": "charge_done"})

    def drv(k):
        L(k, "interrupt_car", {"event_type": "interrupted"})
        _hit("car-", None)
        yield env.timeout(0)

    arrival(env, "car", car, initial=1)
    arrival(env, "driver", drv, gap=3, max_events=1)
    return go(env, log, 15)
