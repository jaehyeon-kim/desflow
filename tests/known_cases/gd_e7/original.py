"""GD-E7: A process returns a value to the process that waits for it. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    # environments.rst, "Miscellaneous": my_proc() returns 42 and other_proc() receives it from yield env.process(my_proc(env))
    def my_proc(k):
        yield env.timeout(1)
        L(k, "my_proc", {"ret": 42})
        return 42

    def other_proc(k):
        ret_val = yield env.process(my_proc(k))
        L(k, "parent", {"event_type": "parent_done"})
        assert ret_val == 42
        L(k, "ok", {"assert": "passed"})

    arrival(env, "a", other_proc, initial=1)
    return go(env, log)
