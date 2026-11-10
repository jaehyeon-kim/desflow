"""GD-TT5: An event and an interrupt arrive together.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

PROCS: dict = {}  # entity key -> its process
UNTIL = 5


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


def _hit(prefix, cause):
    for k, p in list(PROCS.items()):
        if k.startswith(prefix) and p.is_alive:
            p.interrupt(cause)


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    sig = env.event()

    def fox(k):
        while True:
            try:
                yield sig
                break
            except simpy.Interrupt as i:
                L(k, "fox", S("interrupted", cause=i.cause))
        L(k, "fox", {"event_type": "coup_completed"})

    def plan(k):
        L(k, "master_plan", {"event_type": "plan"})
        sig.succeed()
        _hit("fox-", None)
        yield env.timeout(0)

    arrival(env, "fox", fox, initial=size)
    arrival(env, "plan", plan, gap=1, max_events=1)
