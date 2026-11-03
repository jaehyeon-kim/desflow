"""ST-U14: Any of with a failing process that the parent catches. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_util.py test_any_of_with_errors
    env, log, L = new()

    def child_with_error(env, value, k):
        yield env.timeout(value)
        L(k, "child_with_error", S("failed", reason="crashing"))
        raise RuntimeError("crashing")

    def parent(k):
        events = [env.process(child_with_error(env, 1, k)), env.timeout(2, value=2)]
        condition = env.any_of(events)
        try:
            yield condition
            L(k, "pem", {"event_type": "unreachable"})
        except RuntimeError as e:
            assert str(e) == "crashing"
            L(k, "pem", S("failed", reason="crashing"))
            L(k, "caught", {"event_type": "caught", "reason": str(e), "at": env.now})
        assert condition._events[0].value.args[0] == "crashing"
        assert not events[1].processed

    arrival(env, "a", parent, initial=1)
    return go(env, log)
