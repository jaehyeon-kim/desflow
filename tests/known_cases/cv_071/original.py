"""CV-071: Two units held by one process. The plain SimPy program: from the design's SimPy program."""

import simpy
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/Y4.py two_units(): two requests of one process on a resource of two units, released one at a time; `a` requests one after the other and releases the oldest first, `b` requests both at once and releases the newer first
    env, log, L = new()
    m, n = simpy.Resource(env, capacity=2), simpy.Resource(env, capacity=2)

    def seizes(k):
        first = m.request()
        yield first
        L(k, "one", {"e": "one", "x": m.count})
        second = m.request()
        yield second
        L(k, "two", {"e": "both", "x": m.count})
        yield env.timeout(1)
        L(k, "first", {"e": "first"})
        m.release(first)
        L(k, "after_first", {"e": "after_first", "x": m.count})
        yield env.timeout(1)
        L(k, "second", {"e": "second"})
        m.release(second)
        L(k, "after_second", {"e": "after_second", "x": m.count})

    def named(k):
        p, q = n.request(), n.request()
        yield p & q
        L(k, "subject", {"e": "both", "x": n.count})
        yield env.timeout(1)
        L(k, "free_q", {"e": "free_q"})
        n.release(q)
        L(k, "after_q", {"e": "after_q", "x": [n.count, p in n.users]})
        yield env.timeout(1)
        L(k, "free_p", {"e": "free_p"})
        n.release(p)
        L(k, "after_p", {"e": "after_p", "x": n.count})

    arrival(env, "a", seizes, initial=1)
    arrival(env, "b", named, initial=1)
    log, err = go(env, log)
    assert (
        [r[3]["x"] for r in log if r[2] in ("subject", "after_q", "after_p")]
        == [
            2,
            [1, True],
            0,
        ]
    )  # stated (Y4 run_all.out, "Y4 two units"): both held, one after the first release, none after the second
    return log, err
