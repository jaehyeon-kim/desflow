"""EX-3: Machine shop with breakdowns and a repairman.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

PROCS: dict = {}  # entity key -> its process
UNTIL = 3000


def S(status, **fields):
    """A record value that carries a status."""
    return {"status": status, **fields}


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


def build(env: DynamicRealtimeEnvironment, size: int = 10) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    repairman = simpy.PreemptiveResource(env, capacity=1)
    draws = {"part": 0, "gap": 0}

    def part_time():
        draws["part"] += 1
        return 8 + (draws["part"] - 1) % 5

    def break_gap():
        draws["gap"] += 1
        return 250 + (draws["gap"] - 1) % 7 * 10

    class Machine:
        def __init__(self, name):
            self.name = name
            self.broken = False

        def working(self):
            self.process = env.active_process
            env.process(self.break_machine())
            L(self.name, "start_machine", {"event_type": "machine_started"})
            while True:
                done_in = part_time()
                while done_in:
                    start = env.now
                    try:
                        yield env.timeout(done_in)
                        done_in = 0
                    except simpy.Interrupt:
                        L(self.name, "part", S("interrupted", cause=None))
                        L(self.name, "begin_repair", {"event_type": "broken"})
                        self.broken = True
                        done_in -= env.now - start
                        with repairman.request(priority=1) as req:
                            yield req
                            yield env.timeout(30)
                        L(self.name, "repair", {"event_type": "repaired"})
                        self.broken = False
                L(self.name, "part", {"event_type": "part_made"})

        def break_machine(self):
            idx = int(self.name.split()[1])
            while True:
                yield env.timeout(break_gap())
                L(f"break_loop-{idx + 1}", "break_loop", {"event_type": "tick"})
                if not self.broken:
                    self.process.interrupt()

    def machine(k):
        return Machine(k).working()

    def other_jobs(k):
        while True:
            done_in = 30
            while done_in:
                with repairman.request(priority=2) as req:
                    yield req
                    start = env.now
                    try:
                        yield env.timeout(done_in)
                        done_in = 0
                    except simpy.Interrupt as i:
                        c = i.cause
                        by = {p: n for n, p in PROCS.items()}[c.by]
                        L(
                            k,
                            "other_job",
                            S(
                                "preempted",
                                resource="repairman",
                                preempted_by=by,
                                usage_since=c.usage_since,
                            ),
                        )
                        done_in -= env.now - start
            L(k, "other_job", {"event_type": "other_done"})

    count = [0]

    def keyfn():
        k = f"Machine {count[0]}"
        count[0] += 1
        return k

    arrival(env, "machines", machine, initial=size, keyfn=keyfn)
    arrival(env, "others", other_jobs, initial=1)
