"""Machine Shop: SimPy's machine_shop.py example, low-level API.

The low-level version of `builder/simpy/machine_shop_example.py`: SimPy's own
`Machine` and `other_jobs` on a `DynamicRealtimeEnvironment`. They publish with
`publish_event` the records that the YAML and builder versions publish: each task's
`queued`, `started`, `interrupted`, `preempted` and `resumed`, its own events, and the
number of the task's instance. A shop has ten machines, each making parts without end.
Each machine breaks down now and then, unless it is already broken. A broken machine
waits for the one repairman, at priority 1, and then finishes its part. Between
repairs the repairman does other jobs at priority 2, which a repair preempts and which
resume afterwards.

SimPy's example counts minutes, and here each of its minutes is one second. So a part
takes about 10 seconds, a machine breaks down about every 300 seconds, a repair and
another job each take 30 seconds, and the run ends after 40320 seconds, SimPy's four
weeks. Each value is drawn from desflow's `Sampler`, from the random sequence the YAML
and builder versions use for the same place, so the three versions publish the same
records.

Needs no containers. Runs unpaced and ends after 40320 simulation seconds.
"""

import itertools
import logging

import simpy

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

REPAIR_TIME = 30
JOB_DURATION = 30
NUM_MACHINES = 10
UNTIL = 4 * 7 * 24 * 60  # SimPy's four weeks in minutes, read as seconds
REPAIR = "MachineShop.service.repair_time"  # the service a repair's records name
REPAIR_NAMES = {"sim_id": "MachineShop", "service": "repair_time"}
TASKS = ("start_machine", "part", "begin_repair", "repair", "break_loop", "other_job")


def build(env: DynamicRealtimeEnvironment, size: int = NUM_MACHINES) -> None:
    """Starts `size` machines and the repairman's other jobs on `env`."""
    sampler = Sampler(seed=42)
    part = DistributionConfig(dist="truncated_normal", mean=10, std=2, low=0)
    part.stream = "tasks.part.service_time"  # the sequence the blueprint draws from
    failure = DistributionConfig(dist="exponential", rate=0.0033333333333333335)
    failure.stream = "tasks.break_loop.wait.timeout"
    repairman = simpy.PreemptiveResource(env, capacity=1)
    names: dict = {}  # each machine's process, by the name its records carry
    numbers = {task: itertools.count() for task in TASKS}

    def publish(name, task, number, value):
        env.publish_event(name, value, task=task, instance=number)

    class Machine:
        """A machine that makes parts and breaks down now and then."""

        def __init__(self, name, number):
            self.name = name
            self.number = number
            self.broken = False
            self.process = env.process(self.working())
            names[self.process] = name

        def working(self):
            env.process(self.break_machine())
            started = {"event_type": "machine_started"}
            publish(self.name, "start_machine", next(numbers["start_machine"]), started)
            while True:
                making = next(numbers["part"])
                publish(self.name, "part", making, {"status": "started"})
                done_in = sampler.sample(part)
                while done_in:
                    start = env.now
                    try:
                        yield env.timeout(done_in)
                        done_in = 0
                    except simpy.Interrupt:
                        interrupted = {"status": "interrupted", "cause": None}
                        publish(self.name, "part", making, interrupted)
                        broken = {"event_type": "broken"}
                        publish(
                            self.name,
                            "begin_repair",
                            next(numbers["begin_repair"]),
                            broken,
                        )
                        self.broken = True
                        done_in -= env.now - start
                        yield from self.repair()
                        publish(self.name, "part", making, {"status": "resumed"})
                publish(self.name, "part", making, {"event_type": "part_made"})

        def repair(self):
            number = next(numbers["repair"])
            publish(
                self.name,
                "repair",
                number,
                {"path_id": REPAIR, **REPAIR_NAMES, "status": "queued"},
            )
            # The repair goes before the repairman's other jobs
            with repairman.request(priority=1) as req:
                yield req
                publish(
                    self.name,
                    "repair",
                    number,
                    {"path_id": REPAIR, **REPAIR_NAMES, "status": "started"},
                )
                yield env.timeout(REPAIR_TIME)
            publish(self.name, "repair", number, {"event_type": "repaired"})
            self.broken = False

        def break_machine(self):
            name = f"break_loop-{self.number + 1}"
            while True:
                number = next(numbers["break_loop"])
                publish(name, "break_loop", number, {"status": "queued"})
                yield env.timeout(sampler.sample(failure))
                publish(name, "break_loop", number, {"status": "started"})
                publish(name, "break_loop", number, {"event_type": "tick"})
                if not self.broken:
                    self.process.interrupt()

    def other_jobs(name):
        while True:
            number = next(numbers["other_job"])
            publish(name, "other_job", number, {"status": "queued"})
            done_in = JOB_DURATION
            status = "started"
            while done_in:
                with repairman.request(priority=2) as req:
                    yield req
                    publish(name, "other_job", number, {"status": status})
                    start = env.now
                    try:
                        yield env.timeout(done_in)
                        done_in = 0
                    except simpy.Interrupt as interrupt:
                        preempted = interrupt.cause
                        publish(
                            name,
                            "other_job",
                            number,
                            {
                                "status": "preempted",
                                "resource": "repairman",
                                "preempted_by": names[preempted.by],
                                "usage_since": preempted.usage_since,
                            },
                        )
                        done_in -= env.now - start
                        status = "resumed"
            publish(name, "other_job", number, {"event_type": "other_done"})

    for number in range(size):
        Machine(f"Machine {number}", number)
    env.process(other_jobs("others-1"))


def run() -> None:
    """Runs 40320 seconds, SimPy's four weeks counted in minutes."""
    env = DynamicRealtimeEnvironment(factor=0)
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
