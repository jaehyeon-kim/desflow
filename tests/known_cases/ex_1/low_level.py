"""EX-1: Bank with customers who give up waiting.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DistributionConfig, DynamicRealtimeEnvironment, Sampler
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def S(status, **fields):
    """A record value that carries a status."""
    return {"status": status, **fields}


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()
    sampler = Sampler(seed=42)
    gap = DistributionConfig(dist="exponential", rate=0.1)
    gap.stream = (
        "arrival.customers"  # the random sequence the blueprint's arrival draws from
    )
    patiences = sampler.generator("tasks.customer.patience")
    services = sampler.generator("tasks.customer.service_time")

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    def source(number, interval, counter):
        for i in range(number):
            c = customer(env, f"Customer{i:02d}", counter)
            env.process(c)
            t = sampler.sample(gap)
            yield env.timeout(t)

    def customer(env, name, counter):
        arrive = env.now
        with counter.request() as req:
            patience = float(patiences.uniform(1, 3))
            results = yield (req | env.timeout(patience))
            env.now - arrive
            if req in results:
                tib = float(services.exponential(1.0 / 0.08333333333333333))
                yield env.timeout(tib)
                L(name, "customer", {"event_type": "finished"})
            else:
                L(name, "customer", S("reneged"))

    counter = simpy.Resource(env, capacity=1)
    env.process(source(5, 10.0, counter))
