"""GD-P2: Wait for charging and parking together. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def drive(k):
        while True:
            yield env.timeout(30)
            L(k, "drive", {"event_type": "start_parking"})
            charging = env.process(bat_ctrl(k))
            parking = env.timeout(100)
            parking.callbacks.append(lambda e: L(k, "park", {"event_type": "parked"}))
            yield (charging & parking)
            L(k, "run_ev", {"event_type": "stop_parking"})

    def bat_ctrl(k):
        yield env.timeout(50)
        L(k, "bat_ctrl", {"event_type": "bat_done"})

    arrival(env, "ev", drive, initial=1)
    log, err = go(env, log, 310)
    st = [t for t, _, tk, _ in log if tk == "drive"]
    assert st and env.now == 310, log
    assert all((t > st[0] for t, _, tk, _ in log if tk in ("bat_ctrl", "run_ev"))), log
    return (log, err)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    ("service_time: {gen: integer, bounds: [20, 40]}", "service_time: 30"),
    ("service_time: {gen: integer, bounds: [60, 360]}", "service_time: 100"),
    ("service_time: {gen: integer, bounds: [30, 90]}", "service_time: 50"),
]
