"""CV-060: An exception that leaves the run. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def boom(
        k,
    ):  # tests/test_exceptions.py test_no_parent_process: the child raises ValueError('Onoes!') and env.run() raises it
        L(k, "boom", S("failed", reason="Onoes!"))
        raise ValueError("Onoes!")
        yield env.timeout(1)

    arrival(env, "a", boom, initial=1)
    log, err = go(env, log)
    assert (
        err == "ValueError: Onoes!"
    )  # stated (test_no_parent_process): env.run() raises ValueError('Onoes!')
    return log, err
