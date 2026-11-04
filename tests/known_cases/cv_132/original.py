"""CV-132: A process waited for by two waiters. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    sig = env.event()

    def w(k):
        yield env.timeout(2)
        L(k, "worker", {"event_type": "worker_done"})
        if not sig.triggered:
            sig.succeed(7)

    def wa(k):
        yield sig
        L(k, "waiter_a", {"event_type": "a_saw", "at": env.now, "v": sig.value})

    def wb(k):
        yield sig
        L(k, "waiter_b", {"event_type": "b_saw_after", "at": env.now, "v": sig.value})

    arrival(env, "w", w, initial=1)
    arrival(env, "a", wa, initial=1)
    arrival(env, "b", wb, gap=5, max_events=1)
    return go(env, log)
