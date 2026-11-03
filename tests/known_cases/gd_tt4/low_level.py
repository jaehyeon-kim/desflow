"""GD-TT4: Interrupted during service, keeping the unit.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def S(status, **fields):
    """A record value that carries a status."""
    return {"status": status, **fields}


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    res = simpy.Resource(env, 1)

    def worker(env):
        with res.request() as r:
            yield r
            end = env.now + 2
            while env.now < end:
                try:
                    yield env.timeout(end - env.now)
                except simpy.Interrupt as i:
                    L("a-1", "user", S("interrupted", cause=i.cause))
        L("a-1", "user", {"event_type": "user_done"})

    def other(env):
        yield env.timeout(0.5)
        with res.request() as r:
            yield r
            yield env.timeout(1)
        L("b-1", "waiting", {"event_type": "waiting_done"})

    H = {}
    H.update(a=env.process(worker(env)))
    env.process(other(env))

    def intr(env):
        yield env.timeout(1)
        L("i-1", "poke", {"event_type": "poke"})
        H["a"].interrupt()

    env.process(intr(env))
