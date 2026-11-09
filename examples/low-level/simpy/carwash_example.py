"""Carwash: SimPy's carwash.py example, low-level API.

The low-level version of `builder/simpy/carwash_example.py`: SimPy's own processes on
a `DynamicRealtimeEnvironment`. They publish with `publish_event` the records that the
YAML and builder versions publish: each task's `queued` and `started`, its own
events, and the number of the task's instance. A carwash has two washing machines.
Four cars wait when it opens. A car takes a machine, keeps it while the wash runs
(removing 50 to 99 percent of the dirt), and then leaves.

SimPy's example counts minutes, and here each of its minutes is one second. So a car
arrives every 5 to 9 seconds, a wash takes 5 seconds, and the run ends after 20
seconds, SimPy's 20 minutes. Each value is drawn from desflow's `Sampler`, from the
random sequence the YAML and builder versions use for the same place, so the three
versions publish the same records.

Needs no containers. Runs unpaced and ends after 20 simulation seconds.
"""

import itertools
import logging

import simpy

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

NUM_MACHINES = 2
WASHTIME = 5
UNTIL = 20
WASH = "Carwash.service.wash"  # the service that the wash's started record names


def build(env: DynamicRealtimeEnvironment, size: int = 4) -> None:
    """Starts the carwash on `env`, with `size` cars waiting when it opens."""
    # --8<-- [start:machines]
    sampler = Sampler(seed=42)
    gaps = sampler.generator("arrivals.cars.gap")
    dirt = sampler.generator("tasks.wash.payload.dirt_removed_pct")
    machine = simpy.Resource(env, NUM_MACHINES)
    numbers = {task: itertools.count() for task in ("car", "washing", "wash")}
    # --8<-- [end:machines]

    def publish(name, task, number, value):
        env.publish_event(name, value, task=task, instance=number)

    # --8<-- [start:machines-wash]
    def wash(name):
        number = next(numbers["wash"])
        publish(
            name,
            "wash",
            number,
            {
                "path_id": WASH,
                "sim_id": "Carwash",
                "service": "wash",
                "status": "started",
            },
        )
        yield env.timeout(WASHTIME)
        removed = int(dirt.integers(50, 100))
        publish(
            name, "wash", number, {"event_type": "washed", "dirt_removed_pct": removed}
        )

    # --8<-- [end:machines-wash]

    # --8<-- [start:car-is-washed]
    def car(name):
        number = next(numbers["car"])
        with machine.request() as request:
            publish(name, "car", number, {"status": "queued"})
            yield request
            publish(name, "car", number, {"status": "started"})
            publish(name, "car", number, {"event_type": "enters"})
            # The car keeps its machine until the wash has finished
            waiting = next(numbers["washing"])
            publish(name, "washing", waiting, {"status": "queued"})
            yield env.process(wash(name))
            publish(name, "washing", waiting, {"status": "started"})
            publish(name, "washing", waiting, {"event_type": "leaves"})

    # --8<-- [end:car-is-washed]

    # --8<-- [start:cars-arrive]
    def setup():
        for i in range(size):
            env.process(car(f"Car {i}"))
        i = size
        while True:
            yield env.timeout(int(gaps.integers(5, 10)))
            env.process(car(f"Car {i}"))
            i += 1

    env.process(setup())
    # --8<-- [end:cars-arrive]


def run() -> None:
    """Runs 20 simulation seconds."""
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
