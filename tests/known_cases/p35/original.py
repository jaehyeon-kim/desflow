"""P35: Change the queue limit at run time. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # no SimPy original (SimPy has no queue limit; the register ran YAML only: r7_res2.py P35): the limit is checked by hand
    env, log, L = new()
    m = simpy.Resource(env, 1)
    limit = [1]

    def scenario():
        yield env.timeout(2)
        limit[0] = 3

    env.process(scenario())

    def job(k):
        if len(m.queue) >= limit[0]:
            L(k, "job", S("balked", resource="m"))
            return
        with m.request() as r:
            yield r
            yield env.timeout(5)
            L(k, "job", {"event_type": "done"})

    def src():
        for i, t in enumerate((0, 1, 3, 3.5), 1):
            if t > env.now:
                yield env.timeout(t - env.now)
            env.process(job(f"a-{i}"))

    env.process(src())
    log, err = go(env, log, 7)
    # limit 1 at 1 admits the second arrival to the queue, limit 3 from 2 admits the third and fourth, only the first is done by 7
    assert [(x[0], x[1]) for x in log] == [(5, "a-1")], log
    return log, err
