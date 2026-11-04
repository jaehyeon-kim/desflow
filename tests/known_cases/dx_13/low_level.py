"""DX-13: A wait on the same event again after an interrupt.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The `queued` and `started` records are published as the
blueprint publishes them, because the snippet case compares them.
"""

import simpy

from desflow import DynamicRealtimeEnvironment

UNTIL = 20


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    jobs: list = []

    def job(key):
        env.publish_event(key, {"status": "queued"}, task="job")
        limit = env.timeout(5)
        while True:
            try:
                yield limit
                break
            except simpy.Interrupt as interrupt:
                env.publish_event(
                    key,
                    {"status": "interrupted", "cause": interrupt.cause},
                    task="job",
                )
        env.publish_event(key, {"status": "started"}, task="job")
        env.publish_event(key, {"job": "job-1"}, task="job")

    def pause():
        yield env.timeout(2)
        for process in jobs:
            if process.is_alive:
                process.interrupt("pause")

    for number in range(1, size + 1):
        jobs.append(env.process(job(f"a-{number}")))
    env.process(pause())
