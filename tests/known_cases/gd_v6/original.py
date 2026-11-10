"""GD-V6: Any of and all of over a list of events. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # events.rst, "Waiting for multiple events at once": AnyOf and AllOf on a list of events; the guide's list holds plain events that never fire, so the list holds timeouts here
    from simpy.events import AnyOf, AllOf

    env, log, L = new()

    def p(k):
        events = [env.timeout(1), env.timeout(2)]
        yield AllOf(env, events)
        L(k, "both", {"event_type": "all_done"})
        events = [env.timeout(1), env.timeout(2)]
        yield AnyOf(env, events)
        L(k, "either", {"event_type": "any_done"})

    arrival(env, "a", p, initial=1)
    return go(env, log)
