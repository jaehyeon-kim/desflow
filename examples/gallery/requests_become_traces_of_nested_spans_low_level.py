"""Each request becomes a trace of nested spans, and each child span names its parent.

The Low-level form of requests_become_traces_of_nested_spans.yaml, with the same
records, built on `DynamicRealtimeEnvironment`. The gateway span covers an auth check
and a database call that run inside it, so each child starts and ends within its
parent's time.

Needs: nothing
Run it with: python examples/gallery/requests_become_traces_of_nested_spans_low_level.py
"""

import itertools
import logging
import uuid
from datetime import datetime

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

SEED = 42
UNTIL = 120
# The tasks of a request in order: the task, the span's name and its duration range in ms
SPANS = [
    ("auth", "check token", 5, 15),
    ("orders_db", "SELECT orders", 10, 120),
    ("gateway", "GET /orders", 20, 60),
]


def build(env):
    """Starts the request source on `env`."""
    sampler = Sampler(seed=SEED)
    gap = DistributionConfig(dist="exponential", rate=0.5)
    gap.stream = "arrival.requests"
    duration = {
        task: sampler.generator(f"tasks.{task}.vars.duration") for task, *_ in SPANS
    }
    trace_id = sampler.generator("arrivals.requests.attributes.trace_id")
    gateway_span = sampler.generator("arrivals.requests.attributes.gateway_span")
    span_id = {
        task: sampler.generator(f"tasks.{task}.payload.span_id")
        for task, *_ in SPANS
        if task != "gateway"
    }
    # Each task numbers its own instances
    instances = {task: itertools.count() for task, *_ in SPANS}

    def uuid_of(rng):
        return str(uuid.UUID(bytes=rng.bytes(16), version=4))

    def request(key):
        # The trace and the gateway span are the request's own, and the children name them
        trace, gateway, began = uuid_of(trace_id), uuid_of(gateway_span), env.now
        for task, name, low, high in SPANS:
            instance = next(instances[task])
            milliseconds = int(duration[task].integers(low, high + 1))
            env.publish_event(key, {"status": "started"}, task=task, instance=instance)
            yield env.timeout(milliseconds / 1000)
            span = {
                "trace_id": trace,
                "span_id": gateway if task == "gateway" else uuid_of(span_id[task]),
                "parent_span_id": None if task == "gateway" else gateway,
                "name": name,
                "start": began if task == "gateway" else env.now - milliseconds / 1000,
                "duration_ms": (
                    round((env.now - began) * 1000, 3)
                    if task == "gateway"
                    else milliseconds
                ),
            }
            env.publish_event(key, span, task=task, instance=instance)

    def requests():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(request(f"requests-{number}"))

    env.process(requests())


def run():
    """Runs the model for 120 simulation seconds."""
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
