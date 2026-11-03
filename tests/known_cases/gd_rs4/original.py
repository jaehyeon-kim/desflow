"""GD-RS4: Priority resource with a priority for each user. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # resources.rst, PriorityResource: resource_user(name, env, resource, wait, prio) for three processes
    env, log, L = new()

    def resource_user(k, env, resource, wait, prio):
        yield env.timeout(wait)
        with resource.request(priority=prio) as req:
            L(k, "user", {"event_type": "requesting", "priority": prio})
            yield req
            L(k, "get", {"event_type": "got"})
            yield env.timeout(3)
            L(k, "hold", {"event_type": "held"})

    res = simpy.PriorityResource(env, capacity=1)
    arrival(env, "u1", lambda k: resource_user(k, env, res, wait=0, prio=0), initial=1)
    arrival(env, "u2", lambda k: resource_user(k, env, res, wait=1, prio=0), initial=1)
    arrival(env, "u3", lambda k: resource_user(k, env, res, wait=2, prio=-1), initial=1)
    return go(env, log)
