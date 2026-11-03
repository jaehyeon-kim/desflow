"""P36: A priority from an attribute that a step changes. The plain SimPy program: from the program the gap register ran."""

import simpy
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program(n=None, rev=False):
    # research/simpy-gap/audit2/patterns/mine/g_b.py P36 ref(), shape "ties" with 10 rows: a PriorityResource of 1, priority 5 (1 when i % 3 == 0), set to 0 when i % 4 == 0 after the timeout, a grant logged, service 1.
    env, log, L = new()
    m = simpy.PriorityResource(env, 1)
    rows = [(2.0 * (i // 2), i, 5 if i % 3 else 1) for i in range(10)]
    if rev:
        rows = sorted(
            rows, key=lambda x: (x[0], -x[1])
        )  # the rows of one time in the other order, as the runner reverses the file

    def job(name, t, pr, i):
        yield env.timeout(t)
        if i % 4 == 0:
            pr = 0
        with m.request(priority=pr) as r:
            yield r
            L(name, "u", {"event_type": "started"})
            yield env.timeout(1)

    for k, (t, i, pr) in enumerate(rows, 1):
        env.process(job(f"o-{k}", t, pr, i))
    return go(env, log)


program.ties = True
program.sizes = (1,)
