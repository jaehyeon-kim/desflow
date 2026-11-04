"""DX-12: Arguments passed to a child flow and its value read.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The `queued` and `started` records are published as the
blueprint publishes them, because the snippet case compares them.
"""

from desflow import DynamicRealtimeEnvironment

UNTIL = 10


def build(env: DynamicRealtimeEnvironment) -> None:
    def pack(key, boxes, result):
        env.publish_event(key, {"status": "started"}, task="pack")
        yield env.timeout(1)
        value = {"total": boxes * 3}
        env.publish_event(key, value, task="pack")
        result.succeed(value)

    def order(key, boxes):
        env.publish_event(key, {"status": "queued"}, task="wait_pack")
        done = env.event()
        env.process(pack(key, boxes, done))
        packed = yield done
        env.publish_event(key, {"status": "started"}, task="wait_pack")
        env.publish_event(key, {"total": packed["total"] + 1}, task="ship")

    def arrival():
        yield env.timeout(0)
        env.process(order("orders-1", 2))

    env.process(arrival())
