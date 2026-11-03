"""P45: Jockeying between two lines. The plain SimPy program: from the program the gap register ran."""

import simpy
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/simpy-gap/review-r2/b/own/i7_jockey.py simpy_run(3): three customers at 0 ask a (capacity 1); one not served within 1 moves to the end of b (capacity 1) when b has fewer waiting and in use than a has waiting; service 4. The source records the end of each customer's service only.
    env, log, L = new()
    A = simpy.Resource(env, 1)
    B = simpy.Resource(env, 1)

    def cust(k):
        ra = A.request()
        res = yield ra | env.timeout(1)
        if ra in res:
            yield env.timeout(4)
            A.release(ra)
            L(k, "qa", {"event_type": "served_a"})
            return
        if len(B.queue) + B.count < len(A.queue):
            L(
                k, "qa", S("interrupted", cause=None)
            )  # the engine's record of the abandoned wait
            ra.cancel()
            rb = B.request()
            yield rb
            yield env.timeout(4)
            B.release(rb)
            L(k, "qb", {"event_type": "served_b"})
        else:
            yield ra
            yield env.timeout(4)
            A.release(ra)
            L(k, "qa", {"event_type": "served_a"})

    arrival(env, "c", cust, initial=3)
    return go(env, log)
