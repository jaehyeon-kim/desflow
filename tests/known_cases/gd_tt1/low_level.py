"""GD-TT1: Interrupted while joining a child, then join it again.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def S(status, **fields):
    """A record value that carries a status."""
    return {"status": status, **fields}


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    def child():
        yield env.timeout(2)
        L("p-1", "child", {"event_type": "child_done"})

    def parent():
        child_proc = env.process(child())
        try:
            yield child_proc
            raise AssertionError("Did not receive an interrupt.")
        except simpy.Interrupt as i:
            L("p-1", "parent", S("interrupted", cause=i.cause))
            yield child_proc
        L("p-1", "parent", {"event_type": "joined"})

    H = {}
    H.update(p=env.process(parent()))

    def interruptor():
        yield env.timeout(1)
        L("i-1", "interruptor", {"event_type": "interrupt"})
        H["p"].interrupt()

    env.process(interruptor())
