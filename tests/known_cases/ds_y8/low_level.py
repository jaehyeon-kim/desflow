"""DS-Y8: A preempt flag and a priority for each request.

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


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    names = {}

    def jobs(rname, tag):
        r = simpy.PreemptiveResource(env, 1)

        def job(pr):

            def f(k):
                names[env.active_process] = k
                with r.request(priority=pr, preempt=pr < 2) as q:
                    yield q
                    try:
                        yield env.timeout(5)
                        L(k, tag, {"event_type": "done"})
                    except simpy.Interrupt as i:
                        L(
                            k,
                            tag,
                            S(
                                "preempted",
                                resource=rname,
                                preempted_by=names[i.cause.by],
                                usage_since=i.cause.usage_since,
                            ),
                        )

            return f

        return job

    j1 = jobs("r1", "job1")
    j2 = jobs("r2", "job2")
    m = simpy.PriorityResource(env)
    n = simpy.PriorityResource(env)

    def hold(r):

        def f(k):
            with r.request() as q:
                yield q
                yield env.timeout(2)

        return f

    def ask(k):
        x = m.request(priority=0)
        y = n.request(priority=5)
        yield (x & y)
        got = env.now
        yield env.timeout(1)
        m.release(x)
        n.release(y)
        L(k, "ask", {"event_type": "got", "at": got})

    def only_n(k):
        with n.request(priority=1) as q:
            yield q
            got = env.now
            yield env.timeout(1)
            L(k, "only_n", {"event_type": "got", "at": got})

    for nm, f, at in (
        ("low1", j1(5), 0),
        ("high1", j1(1), 1),
        ("low2", j2(5), 0),
        ("high2", j2(3), 1),
        ("hm", hold(m), 0),
        ("hn", hold(n), 0),
        ("p", ask, 0.5),
        ("q", only_n, 1),
    ):
        arrival(env, nm, f, gap=1, max_events=1, first=at)
