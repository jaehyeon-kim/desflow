"""DX-15: A wait for the whole flow of a process.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The `queued` and `started` records are published as the
blueprint publishes them, because the snippet case compares them.
"""

from desflow import DynamicRealtimeEnvironment

UNTIL = 20


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    def flow(key):
        env.publish_event(key, {"status": "started"}, task="relay")
        yield env.timeout(3)
        env.publish_event(key, {"step": "relay"}, task="relay")
        env.publish_event(key, {"status": "started"}, task="more")
        yield env.timeout(3)
        env.publish_event(key, {"step": "more"}, task="more")

    def watch(key, target):
        env.publish_event(key, {"status": "queued"}, task="watch")
        yield target
        env.publish_event(key, {"status": "started"}, task="watch")
        env.publish_event(key, {"step": "done"}, task="watch")

    flows = [env.process(flow(f"a-{number}")) for number in range(1, size + 1)]
    env.process(watch("w-1", flows[0]))
