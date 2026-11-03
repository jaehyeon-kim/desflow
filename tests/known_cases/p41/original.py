"""P41: Renege on a drawn patience. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    m = simpy.Resource(env, 1)

    def jobk(k):
        patience = [3, 1.5, 12][
            int(k.split("-")[1]) % 3 - 1
        ]  # the entity number in the key
        req = m.request()
        got = yield (req | env.timeout(patience))
        if req not in got:
            if req.triggered:
                m.release(req)
            else:
                req.cancel()
            L(k, "job", S("reneged"))
            return
        yield env.timeout(10)
        L(k, "job", {"event_type": "served"})
        m.release(req)

    arrival(env, "a", jobk, initial=3)
    log, err = go(env, log, 20)
    assert [x[0] for x in log if x[3].get("event_type") == "served"] == [10] and all(
        (1 <= x[0] <= 5 for x in log if x[3].get("status") == "reneged")
    ), log
    return (log, err)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    (
        "patience: {gen: sample, dist: uniform, low: 1, high: 5}",
        'patience: {gen: expr, expr: "[3, 1.5, 12][n % 3]", names: {n: {gen: sequence}}}',
    )
]
