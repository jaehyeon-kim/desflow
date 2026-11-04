"""DS-A5: Turn one source off and on from a flow.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment

PROCS: dict = {}  # entity key -> its process
UNTIL = 12


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

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

    env.process(src())
    env.process(ctl())
    env.process(src2())
    env.process(ctl2())
