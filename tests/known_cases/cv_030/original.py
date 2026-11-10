"""CV-030: Schedule an event as urgent. The plain SimPy program: from the design's SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # env.schedule(ev, URGENT, delay): the public method schedules a triggered event with a priority and a delay; no private attribute is set
    env, log, L = new()
    sig = env.event()
    schedule = env.schedule

    def urgent_after_2(
        event, priority=simpy.events.NORMAL, delay=0
    ):  # env.schedule is public: the event the model makes is scheduled with URGENT and delay 2
        if event is sig:
            priority, delay = simpy.events.URGENT, 2
        schedule(event, priority, delay)

    env.schedule = urgent_after_2

    def m(k):
        L(k, "make", {"event_type": "made"})
        sig.succeed()
        yield env.timeout(0)

    def w(k):
        yield sig
        L(k, "wait_x", {"event_type": "got", "at": env.now})

    arrival(env, "m", m, initial=1)
    arrival(env, "w", w, initial=1)
    log, err = go(env, log)
    assert [r[3]["at"] for r in log if r[2] == "wait_x"] == [
        2
    ]  # stated: the event scheduled with URGENT and delay 2 wakes the waiter at time 2
    return log, err
