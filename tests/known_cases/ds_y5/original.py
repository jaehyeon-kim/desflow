"""DS-Y5: Lists and mappings in expressions. The plain SimPy program: from the design's SimPy program."""

import simpy
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import VARS, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/Y5.py counts() (a mapping of counts: seen[k] = seen.get(k, 0) + 1, k in seen), lists() (append, index, len, sum, sorted, a comprehension, a slice) and peek_store() (s.items[0]["v"], len, a comprehension over s.items)
    env, log, L = new()
    seen = {}
    q = []
    s = simpy.Store(env)
    s.put({"v": 1})
    s.put({"v": 2})

    def cnt(k0, d):
        def p(k):
            kk = k0
            seen[kk] = seen.get(kk, 0) + 1
            L(
                k,
                "cnt_log",
                {
                    "event_type": "cnt",
                    "k": kk,
                    "seen": dict(seen),
                    "has_k": kk in seen,
                    "has_z": "z" in seen,
                },
            )
            yield env.timeout(0)

        return p

    def lst(i):
        def p(k):
            q.append(i * 10)
            L(
                k,
                "lst_log",
                {
                    "event_type": "lst",
                    "q": list(q),
                    "first": q[0],
                    "last": q[-1],
                    "n": len(q),
                    "total": sum(q),
                    "ordered": sorted(q, reverse=False),
                    "plus_one": [x + 1 for x in q if x > 0],
                    "rest": q[1:],
                },
            )
            yield env.timeout(0)

        return p

    def peek_store(k):
        L(
            k,
            "peek_store",
            {
                "event_type": "peek_store",
                "first_v": s.items[0]["v"],
                "n": len(s.items),
                "values": [x["v"] for x in s.items],
            },
        )
        yield env.timeout(0)

    for name, k0, d in [("c1", "a", 1), ("c2", "b", 1), ("c3", "a", 2)]:
        arrival(env, name, cnt(k0, d), gap=1, max_events=1, first=d)
    for i in range(3):
        arrival(env, "l%d" % i, lst(i), gap=1, max_events=1, first=i)
    arrival(env, "s", peek_store, gap=1, max_events=1, first=0)
    r = go(env, log)
    VARS["seen"] = seen
    VARS["q"] = q
    return r
