"""DS-V2: A wait timeout computed for each entity.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment, Sampler

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


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    def pair(tag, limit, at):
        ev = env.event()

        def waiter(k):
            lim = env.timeout(limit())
            res = yield (ev | lim)
            L(
                k,
                "waiter" + tag,
                {"event_type": "left", "ev": ev in res, "limit": lim in res},
            )

        def sender(k):
            yield env.timeout(at)
            ev.succeed(1)

        return (waiter, sender)

    limits = {
        name: Sampler(seed=42).generator(f"arrivals.{name}.attributes.limit")
        for name in ("w1", "w2")
    }
    w1, s1 = pair("", lambda: int(limits["w1"].integers(3, 7 + 1)), 1)
    w2, s2 = pair("2", lambda: int(limits["w2"].integers(1, 3 + 1)), 5)
    arrival(env, "w1", w1, gap=1, max_events=1, first=0)
    arrival(env, "s1", s1, gap=1, max_events=1, first=0)
    arrival(env, "w2", w2, gap=1, max_events=1, first=0)
    arrival(env, "s2", s2, gap=1, max_events=1, first=0)
