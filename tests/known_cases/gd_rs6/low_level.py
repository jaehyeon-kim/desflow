"""GD-RS6: A preempt flag for each request.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
from typing import Any
import simpy

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def S(status, **fields):
    """A record value that carries a status."""
    return {"status": status, **fields}


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> Any:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    n = size

    def user(k, env, res, prio, preempt):
        with res.request(priority=prio, preempt=preempt) as req:
            try:
                L(k, "user", {"event_type": "requesting"})
                yield req
                L(k, "get", {"event_type": "got"})
                yield env.timeout(3)
                L(k, "hold", {"event_type": "held"})
            except simpy.Interrupt as i:
                c = i.cause
                who = {p: n for n, p in PROCS.items()}[c.by]
                L(
                    k,
                    "hold",
                    S(
                        "preempted",
                        resource="res",
                        preempted_by=who,
                        usage_since=c.usage_since,
                    ),
                )
                L(k, "preempted", {"event_type": "preempted"})

    res = simpy.PreemptiveResource(env, capacity=1)
    for i in range(n or 1):
        k = f"a-{i + 1}"
        PROCS[k] = env.process(user(k, env, res, prio=0, preempt=True))

    def drive():
        env.run(until=1)
        late = [("late-1", -2, False), ("late-2", -1, True)]
        pass
        for name, prio, preempt in late:
            k = name
            PROCS[k] = env.process(user(k, env, res, prio=prio, preempt=preempt))
        env.run()

    return drive
