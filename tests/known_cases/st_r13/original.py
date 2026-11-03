"""ST-R13: A preempted process waits for an event that never comes. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_preemptive_resource_timeout_0
    env, log, L = new()
    res = simpy.PreemptiveResource(env, 1)
    names = {}

    def proc_a(k, resource, prio):
        names[env.active_process] = k
        with resource.request(priority=prio) as req:
            try:
                yield req
                yield env.timeout(1)
                L(
                    k, "proc_a", {"event_type": "should_not_finish"}
                )  # pytest.fail in the test
            except simpy.Interrupt as i:
                c = i.cause
                L(
                    k,
                    "proc_a",
                    S(
                        "preempted",
                        resource="res",
                        preempted_by=names[c.by],
                        usage_since=c.usage_since,
                    ),
                )
        yield env.event()

    def proc_b(k, resource, prio):
        names[env.active_process] = k
        with resource.request(priority=prio) as req:
            yield req
            L(k, "proc_b", {"event_type": "got"})

    arrival(env, "a", lambda k: proc_a(k, res, 1), initial=1)
    arrival(env, "b", lambda k: proc_b(k, res, 0), initial=1)
    return go(env, log)
