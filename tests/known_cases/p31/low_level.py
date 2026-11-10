"""P31: Hold across several steps and release one of two.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

import simpy

from desflow import DynamicRealtimeEnvironment

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def arrival(
    env, name, make, initial=0, gap=None, max_events=None, first=None, keyfn=None
):
    """Starts `initial` entities now, in order, then one every `gap`, the first
    `first` after the start. `make(key)` returns the process of the entity `key`."""
    count = [0]

    def key():
        count[0] += 1
        return f"{name}-{count[0]}" if keyfn is None else keyfn()

    def begin(label):
        PROCS[label] = env.process(make(label))

    for _ in range(initial):
        begin(key())
    if gap is not None:

        def source():
            made = 0
            if first is not None and first > 0:
                yield env.timeout(first)
            while max_events is None or made < max_events:
                if not (first is not None and made == 0):
                    yield env.timeout(gap() if callable(gap) else gap)
                made += 1
                begin(key())

        env.process(source())


def build(env: DynamicRealtimeEnvironment, size: int = 2) -> None:
    PROCS.clear()
    lathe = simpy.Resource(env, 1)
    press = simpy.Resource(env, 1)

    def job(k):
        on_lathe = lathe.request()
        yield on_lathe
        yield env.timeout(1)
        env.publish_event(k, {"event_type": "lathe"}, task="take_lathe")
        on_press = press.request()
        yield on_press
        yield env.timeout(2)
        env.publish_event(k, {"event_type": "press"}, task="take_press")
        lathe.release(on_lathe)  # the lathe goes back, the press is kept
        yield env.timeout(1)
        env.publish_event(k, {"event_type": "done"}, task="finish")
        press.release(on_press)

    arrival(env, "a", job, initial=size)
