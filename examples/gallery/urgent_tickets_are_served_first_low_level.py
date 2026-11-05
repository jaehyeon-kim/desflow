"""Urgent support tickets are served before older tickets of lower priority.

The Low-level form of urgent_tickets_are_served_first.yaml, with the same records,
built on `DynamicRealtimeEnvironment`. The agent is a `DynamicResource` whose queue
order is `priority`, and each ticket asks for it with its own priority. The priority is
drawn with `Sampler`, from the random sequence the blueprint's generator uses.

Needs: nothing
Run it with: python examples/gallery/urgent_tickets_are_served_first_low_level.py
"""

import itertools
import logging
from datetime import datetime

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

SEED = 42
UNTIL = 14400
PARAMS = SimParameter(
    sim_id="Support",
    arrival={"tickets": DistributionConfig(dist="exponential", rate=0.018)},
    service={"solve": DistributionConfig(dist="normal", mean=50, std=10)},
    resources={"agents": CapacityConfig(current_cap=1, max_cap=1)},
)


def weighted(rng, options):
    """One key of `options`, chosen by its weight, as the `weighted` generator does."""
    point = rng.random() * sum(options.values())
    running = 0.0
    for value, weight in options.items():
        running += weight
        if point < running:
            return value
    return value


def build(env):
    """Starts the ticket source on `env`."""
    env.registry.register_sim_parameter(PARAMS)
    sampler = Sampler(seed=SEED)
    pick = sampler.generator("arrivals.tickets.attributes.priority")
    agents = DynamicResource(env, "Support", "agents", queue="priority")
    path_id = "Support.service.solve"

    def ticket(key, priority, instance):
        def publish(value):
            env.publish_event(key, value, task="solve_ticket", instance=instance)

        publish({"path_id": path_id, "status": "queued"})
        # The lowest priority number is served first
        with agents.request(priority=priority) as request:
            yield request
            publish({"path_id": path_id, "status": "started"})
            yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
            publish({"event_type": "solved", "priority": priority})

    def tickets():
        gap = env.registry.get_config("Support.arrival.tickets")
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            priority = weighted(pick, {1: 10, 2: 30, 3: 60})
            env.process(ticket(f"tickets-{number}", priority, number - 1))

    env.process(tickets())


def run():
    """Runs the model for four simulation hours."""
    env = DynamicRealtimeEnvironment(factor=0, logical_start_time=datetime(2026, 1, 1))
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
