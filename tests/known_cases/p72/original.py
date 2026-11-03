"""P72: A signal that wakes at most a number of waiters. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # review-r1 r16_missing M-trigger1: waiters at 0, 1, 2, fires at 3 and 5, until 10; salabim trigger(max=1) wakes the first waiter that waits (audit2 p67_72.py SimPy form)
    env, log, L = new()
    waiting = []

    def waiter(k):
        ev = env.event()
        waiting.append(ev)
        yield ev
        L(k, "waiter", {"event_type": "woke"})

    def fire(k):
        L(k, "fire", {"event_type": "fire"})
        if waiting:
            waiting.pop(0).succeed()
            L(k, "give", {"event_type": "give"})
        yield env.timeout(0)

    arrival(env, "w", waiter, gap=1, max_events=3, first=0)
    arrival(env, "f", fire, gap=2, max_events=2, first=3)
    return go(env, log, 10)
