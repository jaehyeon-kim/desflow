"""An audit that finds an issue needs a follow-up, which waits for the same auditors.

The Low-level form of audits_with_findings_need_a_follow_up.yaml, with the same records,
built on `DynamicRealtimeEnvironment`. An audit of one of 150 merchants is due about
every 3 hours and takes an auditor about 4 hours, and 15 percent find an issue. A
follow-up of about an hour goes to the back of the same queue.

Needs: nothing
Run it with: python examples/gallery/audits_with_findings_need_a_follow_up_low_level.py
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
UNTIL = 14 * 24 * 3600
PARAMS = SimParameter(
    sim_id="Compliance",
    arrival={"audits_due": DistributionConfig(dist="exponential", rate=0.0001)},
    # DistributionConfig takes seconds: 4 hours (std 1 hour) for an audit,
    # 1 hour (std 15 min) for a follow-up
    service={
        "audit": DistributionConfig(dist="normal", mean=14400, std=3600),
        "follow_up": DistributionConfig(dist="normal", mean=3600, std=900),
    },
    resources={"auditors": CapacityConfig(current_cap=2, max_cap=2)},
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
    """Starts the audit source on `env`."""
    env.registry.register_sim_parameter(PARAMS)
    sampler = Sampler(seed=SEED)
    merchant = sampler.generator("tasks.audit.payload.merchant")
    finding = sampler.generator("tasks.audit.payload.finding")
    auditors = DynamicResource(env, "Compliance", "auditors")
    instances = {"audit": itertools.count(), "follow_up": itertools.count()}

    def step(key, task, payload):
        """Queues for an auditor, works for the task's service time, then publishes
        what `payload()` gives."""
        path_id = f"Compliance.service.{task}"
        instance = next(instances[task])

        def publish(value):
            env.publish_event(key, value, task=task, instance=instance)

        publish({"path_id": path_id, "status": "queued"})
        with auditors.request() as request:
            yield request
            publish({"path_id": path_id, "status": "started"})
            yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
            made = payload()
            publish(made)
        return made

    def audit_payload():
        return {
            "merchant": int(merchant.integers(1, 151)),
            "finding": weighted(finding, {"clean": 85, "issue": 15}),
        }

    def audit(key):
        made = yield from step(key, "audit", audit_payload)
        if made["finding"] == "issue":
            yield from step(
                key,
                "follow_up",
                lambda: {"merchant": made["merchant"], "event_type": "follow_up_done"},
            )

    def audits_due():
        gap = env.registry.get_config("Compliance.arrival.audits_due")
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(audit(f"audits_due-{number}"))

    env.process(audits_due())


def run():
    """Runs the model for 14 simulation days."""
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
