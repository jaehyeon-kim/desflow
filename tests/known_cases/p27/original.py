"""P27: A sub-process with arguments. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def child(k, x):  # yaml-only/programs/V9.py: child(name, x) takes 1 and returns x+1
        yield env.timeout(1)
        L(k, "child", {"v": x + 1})
        return x + 1

    def parent(
        k,
    ):  # k = 2, the first child gets k*3; k = 100, the second child gets k*3; the parent waits for a then b
        a = env.process(child(k, 2 * 3))
        b = env.process(child(k, 100 * 3))
        va = yield a
        vb = yield b
        L(k, "parent", {"event_type": "got", "va": va, "vb": vb})

    arrival(env, "a", parent, initial=1)
    log, err = go(env, log)
    assert [x[3] for x in log if x[2] == "parent"] == [
        {"event_type": "got", "va": 7, "vb": 301}
    ], log  # V9: results 7 and 301 at 1
    return log, err
