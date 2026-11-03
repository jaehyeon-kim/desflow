"""GD-TT6: An unhandled exception stops the run.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment

PROCS: dict = {}  # entity key -> its process
UNTIL = 5


def S(status, **fields):
    """A record value that carries a status."""
    return {"status": status, **fields}


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    def boom(env):
        yield env.timeout(1)
        L("a-1", "boom", S("failed", reason="boom"))
        raise ValueError("boom")

    env.process(boom(env))
