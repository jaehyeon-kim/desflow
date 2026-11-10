"""P70: Log a computed message. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def _seqkeys(start=0):
    c = [start - 1]

    def f():
        c[0] += 1
        return str(c[0])

    return f


def program():
    env, log, L = (
        new()
    )  # no SimPy original (i_run.py I5 ran YAML only): the message is built with the number as SimPy prints it

    def t(k):
        L(k, "t", {"event_type": "log", "message": f"entity {k} at {env.now}"})
        yield env.timeout(0)

    arrival(env, "a", t, gap=1, max_events=3, keyfn=_seqkeys())
    log, err = go(env, log)
    assert [x[3]["message"] for x in log] == [
        "entity 0 at 1",
        "entity 1 at 2",
        "entity 2 at 3",
    ], log  # I5: the message carries the entity and the time
    return log, err
