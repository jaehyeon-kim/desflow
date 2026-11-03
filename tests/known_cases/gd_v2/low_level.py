"""GD-V2: Succeed an event, fail an event and trigger one from another.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment

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

    a, b, c = (env.event(), env.event(), env.event())

    def ok_waiter(k):
        value = yield a
        L(k, "ok_waiter", {"event_type": "ok", "v": value})

    def fail_waiter(k):
        try:
            yield b
            L(k, "fail_waiter", {"event_type": "fine"})
        except ValueError as error:
            L(k, "fail_waiter", S("failed", reason=str(error)))
            L(k, "caught", {"event_type": "caught", "why": str(error)})

    def driver(k):
        yield env.timeout(1)
        L(k, "driver", {"event_type": "drive"})
        a.succeed(7)
        b.fail(ValueError("oops"))
        c.trigger(a)

    arrival(env, "x", ok_waiter, initial=size)
    arrival(env, "y", fail_waiter, initial=1)
    arrival(env, "z", driver, initial=1)
