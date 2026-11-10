"""CV-083: A resource with a grant rule of its own.

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

    state = {"level": 0, "tickets": 0, "turn": 0}
    changed = env.event()

    def poke():
        nonlocal changed
        old, changed = (changed, env.event())
        old.succeed()

    def ticket(amount):

        def run(k):
            i = state["tickets"]
            state["tickets"] += 1
            L(k, "ticket", {"event_type": "ticket"})
            while not (state["level"] >= 1 and state["turn"] == i):
                yield changed
            taken = min(state["level"], amount)
            L(k, "get_it", {"event_type": "got", "taken": taken})
            state["level"] -= taken
            state["turn"] += 1
            poke()

        return run

    def put_it(amount):

        def run(k):
            state["level"] += amount
            L(k, "put_it", {"event_type": "put"})
            poke()
            yield env.timeout(0)

        return run

    arrival(env, "g0", ticket(3), initial=size)
    arrival(env, "g1", ticket(1), initial=1)
    arrival(env, "g2", ticket(4), initial=1)
    arrival(env, "p1", put_it(2), gap=1, max_events=1)
    arrival(env, "p2", put_it(5), gap=2, max_events=1)
