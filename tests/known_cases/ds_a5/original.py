"""DS-A5: Turn one source off and on from a flow. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import go, new, start

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/A5.py half(): gap 1, off at 2.5, on at 5.5; an arrival that finds the source off is dropped.
    # restart(2, 3, 7): gap 2, off at 3, on at 7, until 12; turning the source off drops the pending arrival and turning it on starts a whole new gap. It is written as a wait for the gap or a change of the source in place of restart()'s interrupt, and gives restart()'s arrivals at 2, 9 and 11.
    env, log, L = new()
    st = {"on": True}
    n = [0]

    def src():
        while True:
            yield env.timeout(1)
            if st["on"]:
                n[0] += 1
                L(f"src-{n[0]}", "t", {"event_type": "arrival"})

    def ctl():
        yield env.timeout(2.5)
        st["on"] = False
        L("switch_off-1", "src_off", {"event_type": "off"})
        yield env.timeout(3)
        st["on"] = True
        L("switch_on-1", "src_on", {"event_type": "on"})

    n2 = [0]
    on2 = {"on": True}
    changed2 = [env.event()]

    def switch2(on):
        on2["on"] = on
        event, changed2[0] = changed2[0], env.event()
        event.succeed()

    def src2():
        # off_mode restart: the source waits for the gap or a change of the source, so
        # turning it off drops the pending arrival and turning it on starts a new gap
        while True:
            while not on2["on"]:
                yield changed2[0]
            gap = env.timeout(2)
            yield gap | changed2[0]
            if gap.processed:
                n2[0] += 1
                L(f"src2-{n2[0]}", "t", {"event_type": "arrival"})

    def ctl2():
        yield env.timeout(3)
        switch2(False)
        L("switch2_off-1", "src2_off", {"event_type": "off"})
        yield env.timeout(4)
        switch2(True)
        L("switch2_on-1", "src2_on", {"event_type": "on"})

    start(env, lambda: env.process(src()))
    start(env, lambda: env.process(ctl()))
    start(env, lambda: env.process(src2()))
    start(env, lambda: env.process(ctl2()))
    return go(env, log, 12)
