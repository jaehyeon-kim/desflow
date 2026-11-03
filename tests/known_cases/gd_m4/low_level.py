"""GD-M4: Trace every event the run processes.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment

PROCS: dict = {}  # entity key -> its process
UNTIL = None
TRACE = True  # a second sink receives the trace records


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


def _source(env, name, make, initial):
    """The process a blueprint's arrival is: it starts its entities and ends."""
    if False and None is not None:
        initial = None
    arrival(env, name, make, initial=initial)
    return
    yield


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    PROCS.clear()
    env.trace_events()  # a trace record before each event is processed

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    def test_process(k):
        yield env.timeout(1)
        L(k, "ticking", {"event_type": "tick"})

    env.process(_source(env, "a", test_process, size))
