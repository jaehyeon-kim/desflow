"""CV-021: The events of a condition and at least. The plain SimPy program: from the design's SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/Y3.py timeouts([3, 1, 2], ...) with at_least(), all_of and any_of: timeouts with values d * 10, waited for by a Condition that holds when 2 events are done, by all_of and by any_of
    env, log, L = new()

    def waits(task, condition):
        def p(k):
            evs = [env.timeout(d, value=d * 10) for d in [3, 1, 2]]
            res = yield condition(evs)
            v = [res[e] if e in res else None for e in evs]
            L(
                k,
                task,
                {"event_type": "done", "at": env.now, "a": v[0], "b": v[1], "c": v[2]},
            )

        return p

    def at_least(evs):
        return simpy.events.Condition(env, lambda e, c: c >= 2, evs)

    arrival(env, "a", waits("two_of_three", at_least), initial=1)
    arrival(env, "b", waits("all_three", env.all_of), initial=1)
    arrival(env, "c", waits("any_of_three", env.any_of), initial=1)
    log, err = go(env, log)
    assert (
        [(r[0], r[2], r[3]["a"], r[3]["b"], r[3]["c"]) for r in log]
        == [
            (1, "any_of_three", None, 10, None),
            (2, "two_of_three", None, 10, 20),
            (3, "all_three", 30, 10, 20),
        ]
    )  # stated (Y3 run_all.out): any holds at 1 with b, at least 2 of 3 at 2 with b and c, all at 3 with every value
    return log, err
