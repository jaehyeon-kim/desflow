"""GD-M3: Record the queue length before each request and release.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import (
    CapacityConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    SimParameter,
)

PROCS: dict = {}  # entity key -> its process
UNTIL = None
PARAMS = SimParameter(sim_id="M", resources={"res": CapacityConfig(current_cap=1)})
STATS = "basic"


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


def build(env: DynamicRealtimeEnvironment, size: int = 2) -> None:
    PROCS.clear()
    env.stats.on_change = True  # a reading after each request and release
    res = DynamicResource(env, "M", "res")

    def test_process(k):
        task_id = int(k.split("-")[1]) - 1
        env.stats.flow_started("a")
        env.stats.task_queued("use", task_id)
        with res.request() as req:
            yield req
            env.stats.task_started("use", task_id)
            yield env.timeout(1)
        env.publish_event(k, {"event_type": "used"}, task="use", instance=task_id)
        env.stats.task_finished("use", task_id)
        env.stats.flow_finished("a")

    arrival(env, "a", test_process, initial=size)
