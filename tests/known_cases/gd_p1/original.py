"""GD-P1: A car and a battery controller that pause and wake each other. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    cur = [env.event()]

    def drive(k):
        while True:
            yield env.timeout(30)
            L(k, "drive", {"event_type": "start_parking"})
            e = cur[0]
            cur[0] = env.event()
            e.succeed()
            yield env.timeout(100)
            L(k, "park", {"event_type": "stop_parking"})

    def ctrl(k):
        while True:
            yield cur[0]
            L(k, "passivate", {"event_type": "reactivated"})
            yield env.timeout(50)
            L(k, "charge", {"event_type": "charged"})

    arrival(env, "ev", drive, initial=1)
    arrival(env, "ctrl", ctrl, initial=1)
    log, err = go(env, log, 150)
    rec = {p.get("event_type"): t for t, _, _, p in log}
    assert (
        rec["start_parking"] == rec["reactivated"]
        and rec["charged"] > rec["reactivated"]
        and (env.now == 150)
    ), log
    return (log, err)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    ("service_time: {gen: integer, bounds: [20, 40]}", "service_time: 30"),
    ("service_time: {gen: integer, bounds: [60, 360]}", "service_time: 100"),
    ("service_time: {gen: integer, bounds: [30, 90]}", "service_time: 50"),
]
