"""P55: Match by key with a filter. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # patterns/g_flow.py G / cases/f3.yaml F3b: documents of order 7 at 1 and 8 at 2; a box of order 8 at 0 gets the document of its order (audit2 g_f.py FilterStore form); until 10
    env, log, L = new()
    s = simpy.FilterStore(env)

    def file(o):
        def f(k):
            yield s.put({"order": o})
            L(k, "file", {"order": o})

        return f

    def match(k):
        yield s.get(lambda x: x["order"] == 8)
        L(k, "match", {"matched": 1})

    arrival(env, "d7", file(7), gap=1, max_events=1)
    arrival(env, "d8", file(8), gap=2, max_events=1)
    arrival(env, "box", match, initial=1)
    log, err = go(env, log, 10)
    assert [x[0] for x in log if x[2] == "match"] == [2], (
        log
    )  # the box of order 8 gets its document when order 8 is filed at 2, not the order 7 one at 1
    return log, err
