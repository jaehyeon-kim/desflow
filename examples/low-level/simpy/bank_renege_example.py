"""Bank Renege: SimPy's bank_renege.py example, low-level API.

The low-level version of `builder/simpy/bank_renege_example.py`: SimPy's own processes
on a `DynamicRealtimeEnvironment`. They publish with `publish_event` the records that
the YAML and builder versions publish: each customer's `queued` and `started`, its
`reneged` or `finished`, and the number of the task's instance. One counter serves
five customers. The first arrives at 0 and the others about 10 seconds apart. Each
customer waits for the counter for a patience between 1 and 3 seconds, and goes away
(reneged) if the counter is not free by then. A customer at the counter is served for
about 12 seconds.

SimPy's example counts seconds, as desflow does. Each value is drawn from desflow's
`Sampler`, from the random sequence the YAML and builder versions use for the same
place, so the three versions publish the same records.

Needs no containers. Runs unpaced and ends when the last customer leaves.
"""

import logging

import simpy

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

NEW_CUSTOMERS = 5
UNTIL = None  # the run ends when no event is left


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the customers' source on `env`."""
    # --8<-- [start:draws-and-counter]
    sampler = Sampler(seed=42)
    gap = DistributionConfig(dist="exponential", rate=0.1)
    gap.stream = "arrival.customers"  # the sequence the blueprint's arrival draws from
    patiences = sampler.generator("tasks.customer.patience")
    services = sampler.generator("tasks.customer.service_time")
    counter = simpy.Resource(env, capacity=1)
    # --8<-- [end:draws-and-counter]

    # --8<-- [start:customer-waits-or-reneges]
    def customer(name, number):
        def publish(value):
            env.publish_event(name, value, task="customer", instance=number)

        with counter.request() as req:
            publish({"status": "queued"})
            patience = float(patiences.uniform(1, 3))
            # Waits for the counter at most `patience`, then goes away
            results = yield req | env.timeout(patience)
            if req in results:
                publish({"status": "started"})
                yield env.timeout(float(services.exponential(12.0)))
                publish({"event_type": "finished"})
            else:
                publish({"status": "reneged"})

    # --8<-- [end:customer-waits-or-reneges]

    # --8<-- [start:customers-arrive]
    def source():
        for i in range(NEW_CUSTOMERS):
            env.process(customer(f"Customer{i:02d}", i))
            yield env.timeout(sampler.sample(gap))

    env.process(source())
    # --8<-- [end:customers-arrive]


def run() -> None:
    """Runs until the last customer leaves."""
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
