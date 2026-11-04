"""EX-6: Messages that reach a store after a fixed delay. The plain SimPy program: from the SimPy program.

The three forms are the files of the example `simpy-latency`.
"""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets
EXAMPLE = "latency"  # the forms are the files of SimPy's Event Latency example


def program():
    # latency.py: Cable.latency, sender() and receiver(); the message text uses env.now as the original does (int)
    env, log, L = new()
    n = [0]

    class Cable:
        def __init__(self, env, delay):
            self.env = env
            self.delay = delay
            self.store = simpy.Store(env)

        def latency(self, k, value):
            yield self.env.timeout(self.delay)
            done = self.store.put(
                value
            )  # not waited for, as in the original; the blueprint publishes `delivered` when the put completes
            done.callbacks.append(
                lambda e: L(k, "latency", {"event_type": "delivered"})
            )

        def put(self, k, value):
            n[0] += 1
            self.env.process(self.latency(f"latency-{n[0]}", value))

        def get(self):
            return self.store.get()

    cable = Cable(env, 10)

    def send(k):
        L(k, "send", {"event_type": "sent"})
        cable.put(k, f"Sender sent this at {env.now}")
        yield env.timeout(0)

    def receive(k):
        while True:
            m = yield cable.get()
            L(k, "receive", {"event_type": "received", "msg": m})

    arrival(env, "sender", send, gap=5)
    arrival(env, "receiver", receive, initial=1)
    return go(env, log, 100)
