"""P47: Round robin. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # register audit2/patterns/mine/g_c.py P47: the k-th arrival goes to q((k - 1) % 3 + 1) (the register's reference is that rule, written as a list; entities at one instant)
    env, log, L = new()

    def deal(k):
        L(k, "route", {"event_type": "dealt"})
        q = f"q{(int(k.rsplit('-', 1)[1]) - 1) % 3 + 1}"
        L(k, q, {"event_type": q})
        yield env.timeout(0)

    arrival(env, "a", deal, initial=3)
    return go(env, log)
