"""ST-R07: An exception inside the with block releases the unit.

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

    olog = []

    def process(env, resource, raise_, k, task):
        with resource.request() as req:
            yield req
            yield env.timeout(1)
            olog.append(env.now)
            if raise_:
                try:
                    raise ValueError("Foo")
                except ValueError:
                    pass
            L(k, task, {"event_type": "logged"})

    resource = simpy.Resource(env, 1)
    arrival(
        env,
        "raising",
        lambda k: process(env, resource, True, k, "raising"),
        initial=size,
    )
    arrival(
        env, "plain", lambda k: process(env, resource, False, k, "plain"), initial=1
    )
