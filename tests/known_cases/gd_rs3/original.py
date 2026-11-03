"""GD-RS3: Read count, capacity, users and queue around a request. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # resources.rst, print_stats() and user(): two processes, nothing between the grant and the release
    env, log, L = new()
    res = simpy.Resource(env, capacity=1)

    def print_stats(k, task, event):
        # the example prints count of capacity, the users and the queued events; the lists have no value, so their lengths are recorded
        L(
            k,
            task,
            {
                "event_type": event,
                "count": res.count,
                "queued": len(res.queue),
                "capacity": res.capacity,
            },
        )

    def user(k):
        print_stats(k, "user", "before")
        with res.request() as req:
            yield req
            print_stats(k, "use", "during")
        print_stats(k, "after", "after")

    arrival(env, "a", user, initial=2)
    return go(env, log)
