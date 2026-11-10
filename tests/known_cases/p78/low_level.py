"""P78: Untrap a waiting entity.

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

    door = [env.event()]
    heard = [0]
    flags = {}

    def watcher(stops):

        def run(k):
            flags[k] = 1

            def callback(event, k=k):
                if flags[k] == 1:
                    heard[0] += 1

            door[0].callbacks.append(callback)
            L(
                k,
                "watch" if stops else "keep_listening",
                {"event_type": "watch" if stops else "keep_listening"},
            )
            if stops:
                flags[k] = 0
            return
            yield

        return run

    def ring(k):
        L(k, "ring", {"event_type": "ring"})
        event, door[0] = (door[0], env.event())
        event.succeed()
        return
        yield

    def report(k):
        L(k, "report", {"event_type": "report", "heard": heard[0]})
        return
        yield

    arrival(env, "w2", watcher(False), initial=size)
    arrival(env, "w", watcher(True), initial=1)
    arrival(env, "d", ring, gap=2, max_events=3)
    arrival(env, "r", report, gap=7, max_events=1)
