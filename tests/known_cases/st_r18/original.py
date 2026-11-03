"""ST-R18: A container with no capacity is unbounded. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_initial_container_capacity
    env, log, L = new()
    container = simpy.Container(env)

    def read(k):
        assert container.capacity == float("inf")
        L(
            k,
            "read",
            {
                "event_type": "capacity",
                "capacity": None
                if container.capacity == float("inf")
                else container.capacity,
            },
        )
        yield env.timeout(0)

    arrival(env, "a", read, initial=1)
    return go(env, log)
