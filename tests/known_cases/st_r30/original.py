"""ST-R30: A put into an empty container and two gets. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import MODE, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_immediate_get_request: a put and two gets in turn, with no run in the test
    env, log, L = new()
    c = simpy.Container(env)
    gets = []

    def put_one(k):
        request = c.put(1)
        if not MODE["rev"]:
            assert (
                request.triggered and c.level == 1 and len(c.put_queue) == 0
            )  # the reversed creation order puts the gets first
        yield request
        L(k, "put_one", {"event_type": "put"})  # the yield publishes the grant

    def get_one(k):
        request = c.get(1)
        gets.append(request)
        if MODE["rev"]:
            pass
        elif len(gets) == 1:
            assert request.triggered and c.level == 0 and len(c.get_queue) == 0
        elif len(gets) == 2:
            assert not request.triggered and len(c.get_queue) == 1
        yield request
        L(k, "get_one", {"event_type": "got"})

    def look(k):
        L(
            k,
            "look",
            {
                "event_type": "look",
                "level": c.level,
                "getters": len(c.get_queue),
                "putters": len(c.put_queue),
            },
        )
        return
        yield

    arrival(env, "p", put_one, initial=1)
    arrival(env, "g", get_one, initial=2)
    arrival(env, "look", look, initial=1)
    return go(env, log)
