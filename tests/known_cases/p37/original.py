"""P37: Preemption with resume or restart. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import MODE, PROCS, S, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # register audit2/patterns/mine/g_d1.py P37 (modes resume and restart, priority 1 for i % 3 == 0 else 5, service 1.5), rows of its "ties" set with n=10; each mode has its own machine
    env, log, L = new()
    rows = [(2.0 * (i // 2), 1 if i % 3 == 0 else 5) for i in range(10)]

    def mode_run(name, mode, task):
        m = simpy.PreemptiveResource(env, 1)

        def key_of(proc):
            return next((kk for kk, p in PROCS.items() if p is proc), None)

        def job(k, pr):
            left = 1.5
            while left > 1e-9:
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
                                resource=("m" if mode == "resume" else "m2"),
                                preempted_by=key_of(i.cause.by),
                                usage_since=i.cause.usage_since,
                            ),
                        )
                        if start is not None:
                            left = (
                                (left - (env.now - start)) if mode == "resume" else 1.5
                            )
            L(k, task, {"event_type": "done"})

        def src():
            for n, (t, pr) in enumerate(rows, 1):
                if t > env.now:
                    yield env.timeout(t - env.now)
                k = f"{name}-{n}"
                PROCS[k] = env.process(job(k, pr))

        env.process(src())

    runs = [("r", "resume", "resume_job"), ("q", "restart", "restart_job")]
    for x in reversed(runs) if MODE["rev"] else runs:
        mode_run(
            *x
        )  # the two sources are created in the order of the document's arrivals
    return go(env, log)
