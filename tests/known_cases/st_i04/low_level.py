"""ST-I04: A child interrupted at the instant it is created.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def S(status, **fields):
    """A record value that carries a status."""
    return {"status": status, **fields}


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

    def child(k):
        try:
            yield env.timeout(10)
            L(k, "child", {"event_type": "should_not_finish"})
        except simpy.Interrupt as i:
            L(k, "child", S("interrupted", cause=i.cause))
            L(k, "caught", {"event_type": "interrupted"})

    def root(k):
        L(k, "root", {"event_type": "root"})
        child_proc = env.process(child("child-" + k.rsplit("-", 1)[1]))
        L(k, "stop_child", {"event_type": "stop_child"})
        child_proc.interrupt()
        yield env.timeout(1)
        L(k, "root_wait", {"event_type": "root_done"})

    arrival(env, "root", root, initial=size)
