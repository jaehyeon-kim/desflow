"""GD-V1: A callback added to an event. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import VARS, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # events.rst, "Adding callbacks to an event": my_callback appended to event.callbacks. The guide never triggers the event; here a process fires it at 2 so the callback runs.
    env, log, L = new()
    VARS["seen"] = 0

    def my_callback(event):
        VARS["seen"] = 1  # the guide prints 'Called back from', event

    event = env.event()
    event.callbacks.append(my_callback)

    def fire_it(k):
        yield env.timeout(2)
        L(k, "fire_it", {"event_type": "fired"})
        event.succeed(7)

    arrival(env, "a", fire_it, initial=1)
    return go(env, log)
