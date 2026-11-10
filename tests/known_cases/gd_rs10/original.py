"""GD-RS10: A priority store with text priorities. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # resources.rst, PriorityStore with inspector() and maintainer(); env.run() to the end
    env, log, L = new()
    issues = simpy.PriorityStore(env)

    def inspector(k, env, issues):
        for issue in [
            simpy.PriorityItem("P2", "#0000"),
            simpy.PriorityItem("P0", "#0001"),
            simpy.PriorityItem("P3", "#0002"),
            simpy.PriorityItem("P1", "#0003"),
        ]:
            yield env.timeout(1)
            L(
                k,
                "tick",
                {"event_type": "log", "priority": issue.priority, "item": issue.item},
            )
            yield issues.put(issue)
            L(k, "file", {"event_type": "filed"})

    def maintainer(k, env, issues):
        while True:
            yield env.timeout(3)
            L(k, "wait3", {"event_type": "waited"})
            issue = yield issues.get()
            L(
                k,
                "take",
                {
                    "event_type": "repair",
                    "priority": issue.priority,
                    "item": issue.item,
                },
            )

    arrival(env, "inspector", lambda k: inspector(k, env, issues), initial=1)
    arrival(env, "maintainer", lambda k: maintainer(k, env, issues), initial=1)
    return go(env, log)
