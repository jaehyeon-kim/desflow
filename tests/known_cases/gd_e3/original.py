"""GD-E3: Read the next event time and step one event at a time. The plain SimPy program: from the SimPy program."""

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
    # environments.rst, "Simulation control": until = 10; while env.peek() < until: env.step()
    env, log, L = new()
    arrival(env, "a", _ticking_process(env, L), initial=1)
    flush()
    err = None
    try:
        until = 10
        while env.peek() < until:
            env.step()
    except BaseException as e:
        err = f"{type(e).__name__}: {str(e)[:120]}"
    return log, err
