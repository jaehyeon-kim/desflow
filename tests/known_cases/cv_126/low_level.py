"""CV-126: A computed number of members made during the run.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def build(env: DynamicRealtimeEnvironment, size: int = 3) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    desks: dict = {}

    def job(k, i):
        desk = desks.setdefault(i % 2, simpy.Resource(env, capacity=i + 1))
        with desk.request() as request:
            yield request
            yield env.timeout(2)
            other = desks.get(7)
            x = [
                len(desk.users),
                len(desk.queue),
                0 if other is None else len(other.users),
                sorted(desks),
            ]
            L(k, "job", {"e": "done", "x": x})

    for i in range(size):
        PROCS[f"a-{i + 1}"] = env.process(job(f"a-{i + 1}", i))
