"""CV-093: A last in, first out store. The plain SimPy program: from the design's SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import VARS, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # a last-in, first-out store as a Store subclass whose _do_get pops the last item; items are a counter 0, 1, 2, 3
    class LifoStore(simpy.Store):
        def _do_get(self, event):
            if self.items:
                event.succeed(self.items.pop())

    env, log, L = new()
    s = LifoStore(env)
    n = [0]

    def put(k):
        v = n[0]
        yield s.put(v)
        L(k, "put_it", {"event_type": "put"})
        n[0] = v + 1

    def get(k):
        it = yield s.get()
        L(k, "get_it", {"event_type": "got", "item": it})

    arrival(env, "p", put, gap=1, max_events=4)
    arrival(env, "g", get, gap=6, max_events=4)
    r = go(env, log)
    VARS["n"] = n[0]  # the counter, compared with the document's variable C.variables.n
    return r
