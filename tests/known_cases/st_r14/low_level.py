"""ST-R14: Five processes with priorities and preempt flags.

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

    res = simpy.PreemptiveResource(env, 1)
    names = {}
    olog = []

    def mk(id, delay, prio, preempt):

        def p(k):
            names[env.active_process] = k
            yield env.timeout(delay)
            L(k, "p", {"event_type": "start"})
            with res.request(priority=prio, preempt=preempt) as req:
                try:
                    yield req
                    yield env.timeout(2)
                    olog.append((env.now, id))
                    L(k, "use", {"event_type": "done"})
                except simpy.Interrupt as ir:
                    olog.append((env.now, id, (ir.cause.by, ir.cause.usage_since)))
                    c = ir.cause
                    L(
                        k,
                        "use",
                        S(
                            "preempted",
                            resource="res",
                            preempted_by=names[c.by],
                            usage_since=c.usage_since,
                        ),
                    )
                    L(
                        k,
                        "lost",
                        {
                            "event_type": "preempted",
                            "by": names[c.by],
                            "since": c.usage_since,
                        },
                    )

        return p

    for n, (d, pr, c) in enumerate(
        [(0, 2, True), (0, 2, True), (1, 1, False), (3, 0, True), (4, 3, True)]
    ):
        arrival(env, f"p{n}", mk(n, d, pr, c), initial=size if n == 0 else 1)
