"""DS-M4: Several entities start at one instant, then a source goes on. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new, start

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/M4.py order("setup"), with the check of run(1, 2) first: a setup process starts four cars and goes on as the cars' source; its first gap of 5 ends before the first cars' step of 5, at the same time
    env, log, L = new()

    def proc(first, second, step):
        def p(k):
            L(k, first, {"event_type": "first_step"})
            yield env.timeout(step)
            L(k, second, {"event_type": "goes_on"})

        return p

    arrival(env, "control", proc("check", "check_on", 0), initial=1)
    car = proc("car", "car_on", 5)

    def setup():
        made = 0
        for _ in range(4):
            made += 1
            env.process(car(f"cars-{made}"))
        while True:
            yield env.timeout(5)
            made += 1
            env.process(car(f"cars-{made}"))

    start(env, lambda: env.process(setup()))
    return go(env, log, 12)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [("dist: exponential, rate: 1", "dist: constant, value: 5")]
