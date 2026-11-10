"""ST-X02: A failing child that nobody waits for stops the run. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_exceptions.py test_no_parent_process: the failing child has no waiter, so the error ends run()
    env, log, L = new()

    def child(k):
        L(k, "child", S("failed", reason="Onoes!"))
        raise ValueError("Onoes!")
        yield env.timeout(1)

    def parent(k):
        L(k, "parent", {"event_type": "parent"})
        env.process(child("child-" + k.rsplit("-", 1)[1]))
        yield env.timeout(1)
        L(k, "pause", {"event_type": "parent_done"})

    arrival(env, "p", parent, initial=1)
    r = go(env, log)
    assert r[1] is not None and r[1].startswith(
        "ValueError: Onoes!"
    )  # pytest.raises(ValueError, match='Onoes!')
    return r
