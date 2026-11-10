"""P57: Clone with its own attributes. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    # audit2 p57.py, form "spawn key in two tasks", pre=0: clones c1 (1 s, x=1) and c2 (2 s, x=2), joined; one order at 0
    cnt = {"c1": 0, "c2": 0}

    def child(name, d, x):
        def c(k):
            yield env.timeout(d)
            L(k, name, {})
            L(k, name + "b", {"x": x})

        return c

    def spawn(name, d, x):
        cnt[name] += 1
        return env.process(child(name, d, x)(f"{name}-{cnt[name]}"))

    def split(k):
        L(k, "split", {})
        p1 = spawn("c1", 1, 1)
        L(k, "split2", {})
        p2 = spawn("c2", 2, 2)
        yield env.all_of([p1, p2])
        L(k, "join", {})

    arrival(env, "a", split, initial=1)
    return go(env, log)
