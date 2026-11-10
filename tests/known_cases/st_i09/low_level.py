"""ST-I09: Two interrupts at once while waiting on timeouts of 0.

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

    def proc_a(k):
        timeouts = [env.timeout(0) for i in range(2)]
        names = ["first_wait", "second_wait"]
        while timeouts:
            task = names[2 - len(timeouts)]
            try:
                yield timeouts.pop(0)
                L(k, task, {"event_type": "finished"})
            except simpy.Interrupt as i:
                L(k, task, S("interrupted", cause=i.cause))
        L(k, "end", {"event_type": "end"})

    def proc_b(k):
        for n in range(2):
            for q, a in list(PROCS.items()):
                if q.startswith("a-") and a.is_alive:
                    a.interrupt()
            L(
                k,
                ["poke_twice", "poke_again"][n],
                {"event_type": ["poke", "poke_again"][n]},
            )
        return
        yield

    arrival(env, "a", proc_a, initial=size)
    arrival(env, "b", proc_b, initial=1)
