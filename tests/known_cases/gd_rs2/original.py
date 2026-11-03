"""GD-RS2: Hold one unit with the with form. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # resources.rst, the with form of resource_user
    env, log, L = new()

    def resource_user(k, resource):
        with resource.request() as req:  # Generate a request event
            yield req  # Wait for access
            yield env.timeout(1)  # Do something
            # Resource released automatically
        L(k, "user", {"event_type": "used"})

    res = simpy.Resource(env, capacity=1)
    arrival(env, "a", lambda k: resource_user(k, res), initial=1)
    return go(env, log)
