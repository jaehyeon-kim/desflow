"""P68: A warm-up period. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # patterns/i_run.py I2: warm-up 5, an order every 2, no service, until 9; records stamped before 5 are not kept (YAML only in the register)
    env, log, L = new()

    def t(k):
        if env.now >= 5:
            L(k, "t", {"e": 1})
        yield env.timeout(0)

    arrival(env, "orders", t, gap=2)
    log, err = go(env, log, 9)
    assert [x[0] for x in log] == [6, 8], (
        log
    )  # I2: orders at 2, 4, 6, 8; only those from the warm-up end at 5 are kept
    return log, err
