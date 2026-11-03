"""EX-7a: One generator and one consumer joined by a store. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def message_generator(name, env, out_pipe):
        i = 0
        while True:
            yield env.timeout(8)
            i += 1
            msg = (env.now, f"{name} says hello at {env.now}")
            out_pipe.put(msg)
            L(f"generator-{i}", "speak", {"event_type": "said"})

    def message_consumer(k):
        while True:
            msg = yield pipe.get()
            L(
                k,
                "listen",
                {"event_type": "received", "late": msg[0] < env.now, "text": msg[1]},
            )
            yield env.timeout(6)
            L(k, "busy", {"event_type": "handled"})

    pipe = simpy.Store(env)
    env.process(message_generator("Generator A", env, pipe))
    arrival(env, "consumer", message_consumer, initial=1)
    return go(env, log, 100)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    ("gap: {gen: integer, bounds: [6, 10]}", "gap: 8"),
    ("service_time: {gen: integer, bounds: [4, 8]}", "service_time: 6"),
]
