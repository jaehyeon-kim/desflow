"""ST-I01: A child interrupted at 5 with a cause. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_interrupts.py test_interruption
    env, log, L = new()

    def interruptee(k):
        try:
            yield env.timeout(10)
            L(k, "interruptee", {"event_type": "finished"})
        except simpy.Interrupt as i:
            assert i.cause == "interrupt!"
            L(k, "interruptee", S("interrupted", cause=i.cause))
            L(k, "caught", {"event_type": "interrupted", "cause": i.cause})

    def interruptor(k):
        child_process = env.process(interruptee("interruptee-" + k.rsplit("-", 1)[1]))
        yield env.timeout(5)
        L(k, "interruptor", {"event_type": "waited"})
        L(k, "stop", {"event_type": "interrupt"})
        child_process.interrupt("interrupt!")

    arrival(env, "boss", interruptor, initial=1)
    return go(env, log)
