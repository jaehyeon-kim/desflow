"""P62: A gate on the state. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import VARS, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # patterns/i_run.py I4: an order every 1 waits until variable open is 1; the scenario sets it at 3.5; until 5 (YAML only in the register)
    env, log, L = new()
    op = env.event()
    VARS["open"] = 0

    def t(k):
        if VARS["open"] != 1:
            yield op
        L(k, "t", {"e": 1})

    def scenario():
        yield env.timeout(3.5)
        VARS["open"] = 1
        op.succeed()

    env.process(scenario())
    arrival(env, "orders", t, gap=1)
    return go(env, log, 5)
