"""P37: Preemption with resume or restart.

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

    rows = [(2.0 * (i // 2), 1 if i % 3 == 0 else 5) for i in range(10)]

    def mode_run(name, mode, task):
        m = simpy.PreemptiveResource(env, 1)

        def key_of(proc):
            return next((kk for kk, p in PROCS.items() if p is proc), None)

        def job(k, pr):
            left = 1.5
            while left > 1e-09:
                with m.request(priority=pr) as r:
                    start = None
                    try:
                        yield r
                        start = env.now
                        yield env.timeout(left)
                        left = 0
                    except simpy.Interrupt as i:
                        L(
                            k,
                            task,
                            S(
                                "preempted",
                                resource="m" if mode == "resume" else "m2",
                                preempted_by=key_of(i.cause.by),
                                usage_since=i.cause.usage_since,
                            ),
                        )
                        if start is not None:
                            left = left - (env.now - start) if mode == "resume" else 1.5
            L(k, task, {"event_type": "done"})

        def src():
            for n, (t, pr) in enumerate(rows, 1):
                if t > env.now:
                    yield env.timeout(t - env.now)
                k = f"{name}-{n}"
                PROCS[k] = env.process(job(k, pr))

        env.process(src())

    runs = [("r", "resume", "resume_job"), ("q", "restart", "restart_job")]
    for x in runs:
        mode_run(*x)
