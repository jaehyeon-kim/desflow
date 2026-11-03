"""P28: Try and catch around a step. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    m = simpy.Resource(
        env, 1
    )  # audit2/patterns/mine/g_c.py P28: a cap-1 resource, try: timeout(1); raise RuntimeError("bad") for an even i; except: handled. Rows are scen(10)['ties'] with i = 0..9

    def job(i, t):
        k = f"o-{i + 1}"
        yield env.timeout(t)
        L(
            k, "k", S("queued")
        )  # the blueprint publishes a queued record for every request
        with m.request() as r:
            yield r
            L(k, "k", {"status": "started"})
            try:
                yield env.timeout(1)
                if i % 2 == 0:
                    raise RuntimeError("bad")
                L(k, "k", {"event_type": "ok"})
            except RuntimeError as e:
                L(k, "k", S("failed", reason=str(e)))
                L(k, "handled", {"event_type": "handled"})

    for i in range(10):
        env.process(job(i, 2.0 * (i // 2)))
    return go(env, log)


program.grants = True
