"""GD-S2: Wait until a condition holds.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy
import simpy.rt

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


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    tank = simpy.Container(env, 100, 0)

    def waituntil(env, cond_func, delay=1):
        while not cond_func():
            yield env.timeout(delay)

    def waiter(k):
        yield env.process(waituntil(env, lambda: tank.level >= 30))
        L(k, "waiter", {"event_type": "waituntil_done"})

    def fill(k):
        yield tank.put(10)
        L(k, "fill", {"event_type": "filled"})

    arrival(env, "w", waiter, initial=size)
    arrival(env, "f", fill, gap=1.5, max_events=4)
