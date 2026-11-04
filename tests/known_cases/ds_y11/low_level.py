"""DS-Y11: A callback added during the run.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

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


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    ev, d2, d0 = (env.event(), env.event(), env.event())
    shared = {"cb": 0, "n2": 0, "n0": 0}

    def first_waiter(k):
        value = yield ev
        L(
            k,
            "first_waiter",
            {
                "event_type": "first_waiter",
                "value": value,
                "cb": shared["cb"],
                "now": env.now,
            },
        )

    def attach(k):
        yield env.timeout(1)
        ev.callbacks.append(lambda e: shared.__setitem__("cb", e.value))

    def fire(k):
        yield env.timeout(2)
        ev.succeed(5)

    def late(k):
        yield env.timeout(1.5)
        value = yield ev
        L(
            k,
            "late_wait",
            {
                "event_type": "late_waiter",
                "value": value,
                "cb": shared["cb"],
                "now": env.now,
            },
        )

    def cond2(k):
        d2.callbacks.append(lambda e: shared.__setitem__("n2", shared["n2"] + e.value))
        yield env.timeout(1)
        d2.succeed(3)
        yield env.timeout(1)
        L(k, "cond2_end", {"event_type": "n", "n": shared["n2"]})

    def cond0(k):
        yield env.timeout(1)
        d0.succeed(3)
        yield env.timeout(1)
        L(k, "cond0_end", {"event_type": "n", "n": shared["n0"]})

    for name, run in [
        ("fw", first_waiter),
        ("at", attach),
        ("fi", fire),
        ("lw", late),
        ("p2", cond2),
        ("p0", cond0),
    ]:
        arrival(env, name, run, initial=size if name == "fw" else 1)
