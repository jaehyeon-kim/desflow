"""ST-C15: Two processes wait on the same and condition.

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

    ta = [env.timeout(d, value=d) for d in range(3)]
    c1 = ta[0] & ta[1]
    c2 = c1 & ta[2]
    tb = [env.timeout(d, value=d) for d in range(3)]
    d1 = tb[0] | tb[1]
    d2 = d1 | tb[2]

    def waiter(task, cond, ts, expect):

        def p(k):
            results = yield cond
            L(
                k,
                task,
                {
                    "event_type": "done",
                    "at": env.now,
                    "values": [results[t] for t in ts if t in results],
                    "in_result": [t in results for t in ts],
                },
            )

        return p

    arrival(env, "p1", waiter("and_1", c1, ta[:2], (0, 1)), initial=size)
    arrival(env, "p2", waiter("and_2", c2, ta, (0, 1, 2)), initial=1)
    arrival(env, "p3", waiter("or_1", d1, tb[:2], (0,)), initial=1)
    arrival(env, "p4", waiter("or_2", d2, tb, (0,)), initial=1)
