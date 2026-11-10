"""DS-Y9: Fail an event with a computed reason.

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

    ev, dv = (env.event(), env.event())

    def w(k):
        try:
            value = yield ev
            L(k, "w", {"event_type": "value", "v": value})
        except ValueError as error:
            L(k, "w", S("failed", reason=error.args[0]))
            L(
                k,
                "caught",
                {"event_type": "caught", "why": error.args[0], "at": env.now},
            )

    def f(k):
        yield env.timeout(1)
        ev.fail(ValueError(f"bad {7}"))

    def d(k):
        yield env.timeout(1)
        L(k, "d", {"event_type": "failer_goes_on"})
        dv.fail(ValueError("x"))
        dv.defused = True
        yield env.timeout(1)
        L(k, "d_end", {"event_type": "failer_ended"})

    def c(k):
        yield env.timeout(1)
        try:
            raise KeyError(f"missing {3}")
        except KeyError as error:
            L(k, "c", S("failed", reason=error.args[0]))
            L(
                k,
                "c_caught",
                {"event_type": "caught", "why": error.args[0], "at": env.now},
            )

    arrival(env, "w", w, initial=size)
    arrival(env, "f", f, initial=1)
    arrival(env, "d", d, initial=1)
    arrival(env, "c", c, initial=1)
