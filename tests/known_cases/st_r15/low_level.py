"""ST-R15: Nested preemption over two resources.

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

    res0 = simpy.PreemptiveResource(env, 1)
    res1 = simpy.PreemptiveResource(env, 1)
    names = {}
    olog = []

    def lost(key, ir):
        c = ir.cause
        rn = "res1" if c.resource is res1 else "res0"
        olog.append((env.now, key, (c.by, c.usage_since, c.resource)))
        L(
            key,
            "two_second",
            S(
                "preempted",
                resource=rn,
                preempted_by=names[c.by],
                usage_since=c.usage_since,
            ),
        )
        L(
            key,
            "lost",
            {
                "event_type": "preempted",
                "by": names[c.by],
                "since": c.usage_since,
                "resource": rn,
            },
        )
        L(key, "lost_free_res0", {"event_type": "freed_after_preemption"})

    def process2(key, delay, prio, preempt):
        names[env.active_process] = key
        yield env.timeout(delay)
        with res0.request(priority=prio, preempt=preempt) as req0:
            try:
                yield req0
                L(key, "two_first", {"event_type": "got_res0"})
                done = False
                with res1.request(priority=prio, preempt=preempt) as req1:
                    try:
                        yield req1
                        yield env.timeout(5)
                        olog.append((env.now, key))
                        L(key, "two_second", {"event_type": "done"})
                        done = True
                    except simpy.Interrupt as ir:
                        lost(key, ir)
                if done:
                    L(key, "two_done", {"event_type": "freed_res0"})
            except simpy.Interrupt as ir:
                lost(key, ir)

    def process(key, res, delay, prio, preempt, task):
        names[env.active_process] = key
        yield env.timeout(delay)
        with res.request(priority=prio, preempt=preempt) as req:
            try:
                yield req
                yield env.timeout(5)
                olog.append((env.now, key))
                L(key, task, {"event_type": "done"})
            except simpy.Interrupt as ir:
                olog.append((env.now, key, (ir.cause.by, ir.cause.usage_since)))

    arrival(env, "p0", lambda k: process2(k, 0, -1, True), initial=size)
    arrival(
        env,
        "p1",
        lambda k: process(k, res1, 1, -2, True, "one"),
        gap=1,
        max_events=1,
        first=0,
    )
    arrival(
        env, "p2", lambda k: process2(k, 20, -1, True), gap=1, max_events=1, first=0
    )
    arrival(
        env,
        "p3",
        lambda k: process(k, res0, 21, -2, True, "zero"),
        gap=1,
        max_events=1,
        first=0,
    )
    arrival(
        env, "p4", lambda k: process2(k, 21, -1, True), gap=1, max_events=1, first=0
    )
    {k: p for p, k in names.items()}
