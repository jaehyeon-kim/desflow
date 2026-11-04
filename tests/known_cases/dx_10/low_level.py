"""DX-10: A job that keeps its unit through two steps.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The `queued` and `started` records are published as the
blueprint publishes them, because the snippet case compares them.
"""

import simpy

from desflow import DynamicRealtimeEnvironment

UNTIL = 20


def build(env: DynamicRealtimeEnvironment) -> None:
    machine = simpy.PriorityResource(env, 1)

    def repair(key, name, priority, first, second):
        env.publish_event(key, {"status": "queued"}, task="repair")
        with machine.request(priority=priority) as request:
            yield request
            env.publish_event(key, {"status": "started"}, task="repair")
            yield env.timeout(first)
            env.publish_event(key, {"status": "started"}, task="finish")
            yield env.timeout(second)
            env.publish_event(key, {"job": name}, task="finish")

    def arrival(label, start, name, priority, first, second):
        yield env.timeout(start)
        env.process(repair(f"{label}-1", name, priority, first, second))

    for args in (
        ("j1", 0, "job-1", 2, 1, 2),
        ("j2", 0, "job-2", 1, 2, 1),
        ("j3", 1, "job-3", 0, 1, 1),
    ):
        env.process(arrival(*args))
