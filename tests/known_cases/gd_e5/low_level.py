"""GD-E5: A process asks for the active process and reads it in a function it calls.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The original runs one `env.step()` for each process, which
ends each of them at its first wait. Here the process makes its first pass and ends, so
the run stops when nothing is scheduled.
"""

from desflow import DynamicRealtimeEnvironment

UNTIL = None


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    def subfunc(key):
        env.publish_event(
            key,
            {"event_type": "seen_in_sub", "me": env.active_process.name},
            task="sub",
        )

    def my_proc(key):
        env.publish_event(
            key, {"event_type": "seen", "me": env.active_process.name}, task="my_proc"
        )
        subfunc(key)
        yield env.timeout(0)

    for i in range(size):
        env.process(my_proc(f"p1-{i + 1}"))
