"""ST-R30: A put into an empty container and two gets.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy
import simpy.rt

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

    c = simpy.Container(env)
    gets = []

    def put_one(k):
        request = c.put(1)
        yield request
        L(k, "put_one", {"event_type": "put"})

    def get_one(k):
        request = c.get(1)
        gets.append(request)
        yield request
        L(k, "get_one", {"event_type": "got"})

    def look(k):
        L(
            k,
            "look",
            {
                "event_type": "look",
                "level": c.level,
                "getters": len(c.get_queue),
                "putters": len(c.put_queue),
            },
        )
        return
        yield

    arrival(env, "p", put_one, initial=size)
    arrival(env, "g", get_one, initial=2)
    arrival(env, "look", look, initial=1)
