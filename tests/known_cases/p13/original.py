"""P13: A delay from an attribute. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/simpy-gap/patterns/b_values.py B2 (simmer timeout_from_attribute): sizes 1, 2, 3 arrive every 5; the customer waits 2 * size; run(until=30).
    env, log, L = new()
    sizes = iter((1, 2, 3))

    def t(k):
        size = next(sizes)
        yield env.timeout(2 * size)
        L(k, "t", {"event_type": "by_attribute"})

    arrival(env, "a", t, gap=5, max_events=3)
    return go(env, log, 30)
