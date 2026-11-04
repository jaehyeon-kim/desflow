"""CV-074: Wait on a computed number of requests.

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


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    desks = [simpy.Resource(env, capacity=1) for _ in range(3)]

    def hold(index, seconds):

        def run(k):
            with desks[index].request() as request:
                yield request
                yield env.timeout(seconds)
            L(k, f"hold{index}", {"event_type": f"held{index}"})

        return run

    def pause(k):
        yield env.timeout(0.5)
        L(k, "pause", {"event_type": "paused"})
        requests = [d.request() for d in desks]
        yield env.any_of(requests)
        L(k, "any_desk", {"event_type": "got_one", "at": env.now})
        for index, request in enumerate(requests):
            if request.triggered:
                desks[index].release(request)
            else:
                request.cancel()

    arrival(env, "h0", hold(0, 3), initial=size)
    arrival(env, "h1", hold(1, 2), initial=1)
    arrival(env, "h2", hold(2, 1), initial=1)
    arrival(env, "p", pause, initial=1)
