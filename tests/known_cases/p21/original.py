"""P21: A branch by state. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    state = {"open": 1}

    def route(k):
        L(k, "route", {"event_type": "route"})
        chosen = "closed" if state["open"] == 0 else "fast"
        L(k, chosen, {"event_type": chosen})
        yield env.timeout(0)

    def close_at_3_5():
        yield env.timeout(3.5)
        state["open"] = 0

    env.process(close_at_3_5())
    arrival(env, "a", route, gap=1, max_events=6)
    return go(env, log)
