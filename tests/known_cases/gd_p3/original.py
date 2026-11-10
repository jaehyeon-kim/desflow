"""GD-P3: Interrupt the charging branch of a wait. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def drive(k):
        while True:
            yield env.timeout(30)
            L(k, "drive", {"event_type": "start_parking"})
            charging = env.process(bat_ctrl(k))
            parking = env.timeout(60)
            parking.callbacks.append(lambda e: L(k, "park", {"event_type": "parked"}))
            yield (charging | parking)
            if not charging.triggered:
                charging.interrupt("Need to go!")
            L(k, "run_ev", {"event_type": "stop_parking"})

    def bat_ctrl(k):
        try:
            yield env.timeout(70)
            L(k, "bat_ctrl", {"event_type": "bat_done"})
        except simpy.Interrupt as i:
            L(k, "bat_ctrl", S("interrupted", cause=i.cause))

    arrival(env, "ev", drive, initial=1)
    return go(env, log, 100)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    ("service_time: {gen: integer, bounds: [20, 40]}", "service_time: 30"),
    ("service_time: {gen: integer, bounds: [60, 90]}", "service_time: 70"),
]
