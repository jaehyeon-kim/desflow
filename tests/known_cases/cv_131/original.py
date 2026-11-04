"""CV-131: A callback that interrupts or spawns. The plain SimPy program: from the design's SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    bell = env.event()
    procs = []

    def sleeper(k):
        procs.append(env.active_process)
        try:
            yield env.timeout(10)
            L(k, "sleeper", {"event_type": "slept"})
        except simpy.Interrupt as i:
            L(k, "sleeper", S("interrupted", cause=i.cause))

    def helper(k):
        L(k, "helper", {"event_type": "helper"})
        yield env.timeout(0)

    def on_bell(ev):
        for p in procs:
            if p.is_alive:
                p.interrupt("bell")
        env.process(helper("helper-1"))

    bell.callbacks.append(on_bell)

    def ring(k):
        L(k, "ring", {"event_type": "ring"})
        bell.succeed()
        yield env.timeout(0)

    arrival(env, "s", sleeper, initial=1)
    arrival(env, "r", ring, gap=2, max_events=1)
    log, err = go(env, log)
    assert [
        (r[0], r[2], r[3].get("cause")) for r in log if r[2] in ("sleeper", "helper")
    ] == [
        (2, "sleeper", "bell"),
        (2, "helper", None),
    ]  # stated: the callback interrupts the sleeper and starts the helper at time 2
    return log, err
