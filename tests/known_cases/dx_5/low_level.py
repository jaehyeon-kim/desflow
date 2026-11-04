"""DX-5: A payload that reads which branches of a wait were done.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The signal fires again each time: a waiter holds the event
of the next fire.
"""

from desflow import DynamicRealtimeEnvironment

UNTIL = 60


def build(env: DynamicRealtimeEnvironment) -> None:
    signal = {"next": env.event()}  # the event that the next restock triggers

    def order(key, task_id):
        env.publish_event(key, {"status": "queued"}, task="order", instance=task_id)
        stock = signal["next"]
        limit = env.timeout(30)
        yield stock | limit  # the first of the two branches
        env.publish_event(key, {"status": "started"}, task="order", instance=task_id)
        env.publish_event(
            key,
            {
                "event_type": "resumed",
                "done": {"stock": stock.triggered, "limit": limit.processed},
            },
            task="order",
            instance=task_id,
        )

    def restock(key, task_id):
        fired = signal["next"]
        signal["next"] = env.event()
        fired.succeed()
        env.publish_event(key, {"boxes": 6}, task="restock", instance=task_id)
        yield env.timeout(0)

    def replay(name, times, make):
        now = 0
        for number, at in enumerate(times):
            yield env.timeout(at - now)
            now = at
            env.process(make(f"{name}-{number + 1}", number))

    env.process(replay("orders", [3, 40], order))
    env.process(replay("restocks", [10, 50], restock))
