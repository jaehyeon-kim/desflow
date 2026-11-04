"""CV-114: Raise again after cleanup. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def body(k):
        try:
            L(k, "body", S("failed", reason="body failed"))
            raise RuntimeError("body failed")
        except RuntimeError:
            L(k, "cleanup", S("failed", reason="again"))
            raise RuntimeError("again")
        yield env.timeout(0)

    arrival(env, "a", body, initial=1)
    log, err = go(env, log)
    assert [(r[2], r[3]["reason"]) for r in log] == [
        ("body", "body failed"),
        ("cleanup", "again"),
    ]  # stated: the body fails, then the cleanup fails again with its own reason
    return log, err
