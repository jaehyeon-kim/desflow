"""DS-V12: Put back the item a get took.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

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

    st = simpy.FilterStore(env)
    st.items.extend([{"req": {"id": 7}}, {"req": {"id": 8}}, 5, 0])
    st2 = simpy.Store(env)
    st2.items.append({"d": 3})

    def p(k):
        it = yield st.get(lambda i: isinstance(i, dict) and i["req"]["id"] == 7)
        L(k, "take", {"event_type": "took", "item": it})
        second = yield st.get(lambda i: i == 5)
        L(k, "second", {"event_type": "second", "item": second})
        third = yield st.get(lambda i: i == 0)
        L(k, "third", {"event_type": "third", "item": third})
        yield st.put(it)
        L(k, "back", {"event_type": "back"})
        yield st.put(5)
        L(k, "num", {"event_type": "num"})
        L(k, "report", {"event_type": "items", "items": list(st.items)})

    def after_get(k):
        it = yield st2.get()
        yield env.timeout(it["d"])
        L(k, "after_get", {"event_type": "used", "at": env.now})

    arrival(env, "p", p, gap=3, max_events=1, first=0)
    arrival(env, "q", after_get, gap=3, max_events=1, first=0)
