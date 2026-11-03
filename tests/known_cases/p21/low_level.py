"""P21: A branch by state.

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


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()
    state = {"open": 1}

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    def route(k):
        L(k, "route", {"event_type": "route"})
        chosen = "closed" if state["open"] == 0 else "fast"
        L(k, chosen, {"event_type": chosen})
        yield env.timeout(0)

    def close_at_3_5():
        yield env.timeout(3.5)
        state["open"] = 0

    env.process(close_at_3_5())
    arrival(env, "a", route, gap=1, max_events=6)
