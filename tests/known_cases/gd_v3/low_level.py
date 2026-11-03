"""GD-V3: A shared event made again after each firing.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def arrival(
    env, name, make, initial=0, gap=None, max_events=None, first=None, keyfn=None
):
    """Starts `initial` entities now, in order, then one every `gap`, the first
    `first` after the start. `make(key)` returns the process of the entity `key`."""
    count = [0]

    def key():
        count[0] += 1
        return f"{name}-{count[0]}" if keyfn is None else keyfn()

    def begin(label):
        PROCS[label] = env.process(make(label))

    for _ in range(initial):
        begin(key())
    if gap is not None:

        def source():
            made = 0
            if first is not None and first > 0:
                yield env.timeout(first)
            while max_events is None or made < max_events:
                if not (first is not None and made == 0):
                    yield env.timeout(gap() if callable(gap) else gap)
                made += 1
                begin(key())

        env.process(source())


def build(env: DynamicRealtimeEnvironment, size: int = 3) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    class School:
        def __init__(self, env):
            self.env = env
            self.class_ends = env.event()

        def bell(self):
            for i in range(2):
                yield self.env.timeout(45)
                L(f"bell-{i + 1}", "ring", {"event_type": "bell"})
                self.class_ends.succeed()
                self.class_ends = self.env.event()

        def pupil(self, k):
            for i in range(2):
                yield self.class_ends
                L(
                    k,
                    ["first_class", "second_class"][i],
                    {"event_type": ["class_over", "class_over_again"][i]},
                )

    school = School(env)
    arrival(env, "pupils", school.pupil, initial=size)
    env.process(school.bell())
