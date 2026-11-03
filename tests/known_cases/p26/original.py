"""P26: A sub-process that returns a value. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def a(
        k,
    ):  # c_routing.py C4: child(env, x): yield timeout(x); return x*10; parent: v = yield env.process(child(env, 2))
        def child(x):
            yield env.timeout(x)
            L(k, "child", {"v": x * 10})
            return {"v": x * 10}

        v = yield env.process(child(2))
        L(k, "parent", {"event_type": "got", "v": v["v"]})

    arrival(env, "a", a, initial=1)
    log, err = go(env, log)
    assert [(x[0], x[3]["v"]) for x in log if x[2] == "parent"] == [(2, 20)], (
        log
    )  # C4 prints [(2, 20)]
    return log, err
