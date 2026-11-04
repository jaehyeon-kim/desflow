"""CV-112: Handle a failure by its type.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def S(status, **fields):
    """A record value that carries a status."""
    return {"status": status, **fields}


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


def build(env: DynamicRealtimeEnvironment, size: int = 3) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    kinds = ["KeyError", "ValueError", "ValueError"]
    classes = {"KeyError": KeyError, "ValueError": ValueError}
    count = [0]

    def step(k):
        kind = kinds[count[0] % 3]
        count[0] += 1
        try:
            raise classes[kind](kind)
        except KeyError as error:
            L(k, "step", S("failed", reason=error.args[0]))
            L(k, "handle", {"event_type": "handled"})
            L(k, "key_handler", {"event_type": "key_error"})
        except ValueError as error:
            L(k, "step", S("failed", reason=error.args[0]))
            L(k, "handle", {"event_type": "handled"})
            L(k, "other_handler", {"event_type": "other"})
        return
        yield

    arrival(env, "a", step, initial=size)
