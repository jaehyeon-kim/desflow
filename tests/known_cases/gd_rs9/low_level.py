"""GD-RS9: A filter store of machines.

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

    from collections import namedtuple

    Machine = namedtuple("Machine", "size, duration")
    m1 = Machine(1, 2)
    m2 = Machine(2, 1)
    machine_shop = simpy.FilterStore(env, capacity=2)
    machine_shop.items = [m1, m2]

    def user(k, env, ms, size):
        machine = yield ms.get(lambda machine: machine.size == size)
        L(
            k,
            "take",
            {"event_type": "took", "size": machine.size, "duration": machine.duration},
        )
        yield env.timeout(machine.duration)
        L(k, "use", {"event_type": "used"})
        yield ms.put(machine)
        L(k, "back", {"event_type": "released"})

    for i in range(3):
        arrival(
            env,
            f"u{i}",
            lambda k, i=i: user(k, env, machine_shop, i % 2 + 1),
            initial=size if i == 0 else 1,
        )
