"""CV-139: Text built from values. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def say(k):
        L(k, "say", {"event_type": "say", "text": "item 3", "text2": "count 3"})
        yield env.timeout(0)

    arrival(env, "a", say, initial=1)
    log, err = go(env, log)
    assert (log[0][3]["text"], log[0][3]["text2"]) == (
        "item 3",
        "count 3",
    )  # stated: text built from values
    return log, err
