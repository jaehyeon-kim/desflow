"""ST-P05: A child returns an exception object as a value. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_process.py test_child_exception
    env, log, L = new()

    def child(k):
        yield env.timeout(1)
        L(k, "child", {"error": "Onoes!"})
        return RuntimeError("Onoes!")

    def parent(k):
        result = yield env.process(child(k))
        assert isinstance(result, Exception)
        L(k, "parent", {"event_type": "got", "value": str(result)})

    arrival(env, "p", parent, initial=1)
    return go(env, log)
