"""GD-RS5: Preemptive resource that reports who preempted. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import PROCS, S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # resources.rst, PreemptiveResource: resource_user(name, env, resource, wait, prio) for three processes
    env, log, L = new()

    def resource_user(k, env, resource, wait, prio):
        yield env.timeout(wait)
        with resource.request(priority=prio) as req:
            L(k, "user", {"event_type": "requesting", "priority": prio})
            yield req
            L(k, "get", {"event_type": "got"})
            try:
                yield env.timeout(3)
                L(k, "hold", {"event_type": "held"})
            except simpy.Interrupt as interrupt:
                by = interrupt.cause.by
                usage = env.now - interrupt.cause.usage_since
                who = {p: n for n, p in PROCS.items()}[by]
                L(
                    k,
                    "hold",
                    S(
                        "preempted",
                        resource="res",
                        preempted_by=who,
                        usage_since=interrupt.cause.usage_since,
                    ),
                )
                L(
                    k,
                    "preempted",
                    {"event_type": "preempted", "by": who, "usage": usage},
                )

    res = simpy.PreemptiveResource(env, capacity=1)
    arrival(env, "u1", lambda k: resource_user(k, env, res, wait=0, prio=0), initial=1)
    arrival(env, "u2", lambda k: resource_user(k, env, res, wait=1, prio=0), initial=1)
    arrival(env, "u3", lambda k: resource_user(k, env, res, wait=2, prio=-1), initial=1)
    return go(env, log)
