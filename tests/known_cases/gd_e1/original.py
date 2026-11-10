"""GD-E1: Run the model in segments to a series of times. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, flush, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def _ticking_process(env, L):
    # environments.rst has no model; one process that ticks every 1 gives the loops something to run
    def ticking(k):
        while True:
            yield env.timeout(1)
            L(k, "ticking", {"event_type": "tick"})

    return ticking


def program():
    # environments.rst, "Simulation control": for i in range(100): env.run(until=i). The guide's loop fails at i = 0
    # (ValueError: until (0) must be greater than the current simulation time), so it starts at 1.
    env, log, L = new()
    arrival(env, "a", _ticking_process(env, L), initial=1)
    flush()
    err = None
    try:
        for i in range(1, 100):
            env.run(until=i)
    except BaseException as e:
        err = f"{type(e).__name__}: {str(e)[:120]}"
    return log, err
