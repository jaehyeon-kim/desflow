"""ST-U04: A subscriber interrupted while waiting for another child. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_subscribe_with_join
    env, log, L = new()
    import simpy.util

    def child(env, i, k, task):
        yield env.timeout(i)
        L(k, task, {"event_type": "c%d" % i})

    def parent(k):
        child_proc1 = env.process(child(env, 1, k, "child1"))
        child_proc2 = env.process(child(env, 2, k, "child2"))
        try:
            simpy.util.subscribe_at(child_proc1)
            yield child_proc2
            L(k, "parent", {"event_type": "joined"})
        except simpy.Interrupt as interrupt:
            assert env.now == 1
            assert interrupt.cause is not None
            assert interrupt.cause[0] is child_proc1
            assert child_proc2.is_alive
            L(
                k,
                "parent",
                S(
                    "interrupted",
                    cause={"event": "child1", "value": {"event_type": "c1"}},
                ),
            )
            L(k, "told", {"event_type": "interrupted", "at": env.now})

    arrival(env, "p", parent, initial=1)
    return go(env, log)
