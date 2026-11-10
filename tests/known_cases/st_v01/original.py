"""ST-V01: A parent succeeds an event that a child waits for. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_event.py test_succeed
    env, log, L = new()

    def child(k, event):
        value = yield event
        assert value == "ohai" and env.now == 5
        L(k, "child", {"event_type": "got", "value": value})

    cnt = [0]

    def parent(k):
        event = env.event()
        cnt[0] += 1
        env.process(child(f"child-{cnt[0]}", event))
        yield env.timeout(5)
        L(k, "parent", {"event_type": "parent"})
        event.succeed("ohai")

    arrival(env, "p", parent, initial=1)
    return go(env, log)
