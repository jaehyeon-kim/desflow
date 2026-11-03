"""GD-E6: Event, process, timeout, all of and any of through the environment. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    ev = env.event()

    def a(k):
        def child():
            yield env.timeout(2)
            L(k, "child", {"event_type": "child_done"})
            ev.succeed()

        c = env.process(child())
        yield env.timeout(3) & c & ev
        L(k, "use_all", {"event_type": "all_done"})
        yield env.timeout(1) | ev
        L(k, "use_any", {"event_type": "any_done"})

    arrival(env, "a", a, initial=1)
    return go(env, log)
