"""P14: A delay from the state. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # no SimPy program exists for a delay from state or from the time (b_values.py has YAML and builder forms only); written from the description: every 5 an entity waits the state value 3, then 1 + now / 10; run(until=30).
    env, log, L = new()
    slow = 3

    def u(k):
        yield env.timeout(slow)
        L(k, "u", {"event_type": "by_state"})
        yield env.timeout(1 + env.now / 10)
        L(k, "v", {"event_type": "by_time"})

    arrival(env, "a", u, gap=5, max_events=3)
    return go(env, log, 30)
