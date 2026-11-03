"""P09: Switch one source off and on.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment

PROCS: dict = {}  # entity key -> its process
UNTIL = 8


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    on = [True]
    n = [0]

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

    env.process(src())
    env.process(ctl())
