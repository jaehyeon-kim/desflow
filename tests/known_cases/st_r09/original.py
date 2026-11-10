"""ST-R09: Priority resource with four requests. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import MODE, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_resource_with_priority_queue
    env, log, L = new()
    resource = simpy.PriorityResource(env, capacity=1)

    def mk(delay, priority, res_time):
        def process(k):
            yield env.timeout(delay)
            L(k, "pem", {"event_type": "arrived"})
            req = resource.request(priority=priority)
            yield req
            if not MODE["rev"]:
                assert (
                    env.now == res_time
                )  # the reversed creation order swaps the two equal priorities
            yield env.timeout(5)
            resource.release(req)
            L(k, "use", {"event_type": "granted_and_done"})

        return process

    for nm, d, p, t in (
        ("p0", 0, 2, 0),
        ("p1", 2, 3, 10),
        ("p2", 2, 3, 15),
        ("p3", 4, 1, 5),
    ):
        arrival(env, nm, mk(d, p, t), initial=1)
    return go(env, log)
