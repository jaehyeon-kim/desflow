"""DS-B1: A failure clock that counts busy time only.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

PROCS: dict = {}  # entity key -> its process
UNTIL = 40


def S(status, **fields):
    """A record value that carries a status."""
    return {"status": status, **fields}


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    ttf, repair = (5, 2)
    m = simpy.PreemptiveResource(env, 1)
    left, holding = ([ttf], [0])
    start, stop = ([env.event()], [env.event()])

    def on():
        holding[0] = 1
        e = start[0]
        start[0] = env.event()
        e.succeed()

    def off():
        holding[0] = 0
        e = stop[0]
        stop[0] = env.event()
        e.succeed()

    def job(name, at, service):
        yield env.timeout(at)
        r = m.request(priority=1)
        yield r
        on()
        rem = service
        while rem > 0:
            began = env.now
            try:
                yield env.timeout(rem)
                rem = 0
            except simpy.Interrupt:
                rem -= env.now - began
                L(name, "job", {"status": "interrupted", "cause": "breakdown"})
                r = m.request(priority=0)
                yield r
                on()
        L(name, "job", {"event_type": "done"})
        off()
        m.release(r)

    def clock():
        while True:
            if not holding[0]:
                yield start[0]
            began = env.now
            to = env.timeout(left[0])
            stp = stop[0]
            yield (to | stp)
            left[0] -= env.now - began
            if left[0] <= 1e-12:
                holding[0] = 0
                with m.request(priority=-1) as q:
                    L("m", None, S("down", reason="failure"))
                    yield q
                    yield env.timeout(repair)
                    L("m", None, S("repaired", reason="failure"))
                left[0] = ttf

    env.process(clock())
    for i in range(5):
        env.process(job(f"a-{i + 1}", 4 * i, 3))
