"""CV-116: A nested generator. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/Y6.py yield_from([1, 2]): sub(d) waits d and returns d * 2; p adds the results with yield from
    env, log, L = new()

    def sub(d):
        yield env.timeout(d)
        return d * 2

    def a(k):
        L(k, "main", {"event_type": "main"})
        total = 0
        for i, d in enumerate([1, 2]):
            total += yield from sub(d)
            L(k, "shared", {"event_type": "shared"})
            if i == 0:
                L(k, "first_return", {"event_type": "first_return"})
        L(
            k,
            "second_return",
            {"event_type": "second_return", "at": env.now, "total": total},
        )

    arrival(env, "a", a, initial=1)
    return go(env, log)
