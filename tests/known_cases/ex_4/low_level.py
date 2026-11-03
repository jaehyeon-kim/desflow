"""EX-4: Movie tickets with a sold-out event that makes people leave.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

PROCS: dict = {}  # entity key -> its process
UNTIL = 120


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


def _number(key):
    """The number an entity key ends in, from 1."""
    return int(key.rsplit("-", 1)[1])


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    movies = ["Python Unchained", "Kill Process", "Pulp Implementation"]
    counter = simpy.Resource(env, capacity=1)
    available = {m: 50 for m in movies}
    sold_out = {m: env.event() for m in movies}

    def moviegoer(k):
        n = _number(k) - 1
        movie = movies[n * 7 % 3 if n % 5 else 0]
        tickets = 1 + n * 5 % 6
        if available[movie] == 0:
            L(k, "moviegoer", {"status": "balked"})
            return
        turn = counter.request()
        result = yield (turn | sold_out[movie])
        L(k, "moviegoer", {"event_type": "at_counter"})
        if turn not in result:
            turn.cancel()
            if turn.triggered:
                counter.release(turn)
            L(k, "leave", {"event_type": "reneged"})
            return
        L(k, "serve", {"event_type": "served"})
        if available[movie] < tickets:
            yield env.timeout(0.5)
            L(k, "argue", {"event_type": "argued"})
            counter.release(turn)
            return
        L(k, "buy", {"event_type": "bought"})
        available[movie] -= tickets
        L(k, "check", {"event_type": "checked"})
        if available[movie] < 2:
            L(k, "sold", {"event_type": "sold_out"})
            sold_out[movie].succeed()
            available[movie] = 0
        yield env.timeout(1)
        L(k, "finish", {"event_type": "done"})
        counter.release(turn)

    arrival(env, "goers", moviegoer, gap=0.5)
