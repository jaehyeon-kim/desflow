"""P09: Switch one source off and on. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import go, new, start

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    on = [True]
    n = [
        0
    ]  # yaml-only/programs/A5.py half(): source made first, then the control; gap 1, off at 2.5, on at 5.5, run(until=8)

    def t(k):
        L(k, "t", {"event_type": "arrival"})
        yield env.timeout(0)

    def src():
        while True:
            yield env.timeout(1)
            if on[0]:
                n[0] += 1
                env.process(t(f"src-{n[0]}"))

    def ctl():
        yield env.timeout(2.5)
        L("off-1", "src_off", {"event_type": "off"})
        on[0] = False
        yield env.timeout(3)
        L("on-1", "src_on", {"event_type": "on"})
        on[0] = True

    start(env, lambda: env.process(src()))
    start(env, lambda: env.process(ctl()))
    return go(env, log, 8)
