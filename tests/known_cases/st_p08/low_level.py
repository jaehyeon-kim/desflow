"""ST-P08: A failed child that nobody waits for any more stops the run.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

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

    def child_a(k, process):
        L(k, "child_a", {"event_type": "a"})
        process.interrupt()
        return
        yield

    def child_b(k):
        L(k, "child_b", S("failed", reason="spam"))
        raise AttributeError("spam")
        yield

    def parent(k):
        L(k, "parent", {"event_type": "parent"})
        env.process(child_a(f"child_a-{k.rsplit('-', 1)[1]}", env.active_process))
        L(k, "start_b", {"event_type": "start_b"})
        b = env.process(child_b(k))
        try:
            yield b
        except simpy.Interrupt as i:
            L(k, "join", S("interrupted", cause=i.cause))
        yield env.timeout(0)
        L(k, "after", {"event_type": "after"})

    arrival(env, "p", parent, initial=size)
