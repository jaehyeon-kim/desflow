"""ST-C17: The values of a condition.

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

    def pem(k):
        timeouts = [env.timeout(delay, value=delay) for delay in range(3)]
        results = yield env.all_of(timeouts)
        L(
            k,
            "value",
            {
                "event_type": "done",
                "at": env.now,
                "values": list(results.values()),
                "has_first": timeouts[0] in results,
                "first": results[timeouts[0]],
            },
        )
        plain = [env.timeout(delay) for delay in range(3)]
        name = {t: f"t{i}" for i, t in enumerate(plain)}
        timeouts = list(reversed(plain))
        results = yield env.all_of(timeouts)
        L(
            k,
            "result_order",
            {"event_type": "done", "order": [name[e] for e in results.keys()]},
        )
        timeouts = [env.timeout(delay) for delay in range(3)]
        name = {t: f"t{i}" for i, t in enumerate(timeouts)}
        condition = (timeouts[0] | timeouts[1]) & timeouts[2]
        results = yield condition
        L(
            k,
            "nested_result_order",
            {"event_type": "done", "order": [name[e] for e in results.keys()]},
        )

    arrival(env, "a", pem, initial=size)
