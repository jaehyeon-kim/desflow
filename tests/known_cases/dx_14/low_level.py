"""DX-14: A request yielded again after an interrupt.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The `queued` and `started` records are published as the
blueprint publishes them, because the snippet case compares them.
"""

import simpy

from desflow import DynamicRealtimeEnvironment

UNTIL = 20


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    machine = simpy.Resource(env, 1)
    first_process: list = []

    def holder(key):
        env.publish_event(key, {"status": "queued"}, task="holder")
        with machine.request() as request:
            yield request
            env.publish_event(key, {"status": "started"}, task="holder")
            yield env.timeout(5)
            env.publish_event(key, {"job": "holder"}, task="holder")

    def first(key):
        env.publish_event(key, {"status": "queued"}, task="first")
        request = machine.request()
        env.publish_event(key, {"status": "queued"}, task="first")
        while True:
            try:
                yield request
                break
            except simpy.Interrupt as interrupt:
                env.publish_event(
                    key,
                    {"status": "interrupted", "cause": interrupt.cause},
                    task="first",
                )
        env.publish_event(key, {"status": "started"}, task="first")
        env.publish_event(key, {"status": "started"}, task="first")
        env.publish_event(key, {"status": "started"}, task="first_hold")
        yield env.timeout(1)
        machine.release(request)
        env.publish_event(key, {"job": "first"}, task="first_hold")

    def second(key):
        env.publish_event(key, {"status": "queued"}, task="second")
        with machine.request() as request:
            yield request
            env.publish_event(key, {"status": "started"}, task="second")
            yield env.timeout(1)
            env.publish_event(key, {"job": "second"}, task="second")

    def later(delay, label, make):
        yield env.timeout(delay)
        env.process(make(f"{label}-1"))

    def pause():
        yield env.timeout(2)
        first_process[0].interrupt("pause")

    for number in range(1, size + 1):
        env.process(holder(f"h-{number}"))
    first_process.append(env.process(first("f-1")))
    env.process(later(1, "s", second))
    env.process(pause())
