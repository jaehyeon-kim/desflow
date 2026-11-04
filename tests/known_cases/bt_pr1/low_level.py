"""BT-PR1 to BT-PR10: product-recommender.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The specs of the generators are those of `builder.py`; each
payload is rendered by a `Template` named for the task, with a `ValueRuntime` of the
document's seed, so every field draws from the random sequence the document's generator
at its place draws from. The users are kept for the `lookup` of the history events.
"""

from datetime import datetime, timezone
from typing import Any

from desflow import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
    SimParameter,
)
from desflow.values import Template, ValueRuntime

from tests.known_cases.bt_pr1.builder import TASKS, deep
from tests.known_cases.bt_support import Context, Entity

UNTIL = 7776000
PARAMS = SimParameter(
    sim_id="Rec",
    arrival={"history": DistributionConfig(dist="exponential", rate=0.001286)},
)
ENVIRONMENT = {
    "logical_start_time": datetime(2025, 10, 3, tzinfo=timezone.utc),
    "timezone": "UTC",
}


def build(env: DynamicRealtimeEnvironment) -> None:
    sampler = Sampler(seed=42)
    values = ValueRuntime(42, env, sampler, None)
    context = Context(values)
    templates = {
        name: Template(
            deep(spec.get("payload", {})),
            name=f"tasks.{name}",
            vars=deep(spec.get("vars")),
        )
        for name, spec in TASKS.items()
    }
    values.keep_rows("user", 1000)
    counts = {name: 0 for name in TASKS}

    def publish(name: str, key: str, entity: Any) -> dict:
        payload = templates[name].render(context, entity)
        values.record(name, payload, entity)
        env.publish_event(key, payload, task=name, instance=counts[name])
        counts[name] += 1
        return payload

    def users() -> Any:
        for number in range(1, 1001):
            yield env.timeout(1)
            key = f"users-{number}"
            publish("user", key, Entity(key))

    def history() -> Any:
        config = env.registry.get_config("Rec.arrival.history")
        config.stream = "arrival.history"
        for number in range(1, 10001):
            yield env.timeout(sampler.sample(config))
            key = f"history-{number}"
            publish("event", key, Entity(key))

    def live() -> Any:
        for number in range(1, 51):
            yield env.timeout(0.1)
            entity = Entity(f"live-{number}")
            payload = templates["rank"].render(context, entity)
            env.publish_event(
                payload["product_id"], payload, task="rank", instance=counts["rank"]
            )
            counts["rank"] += 1

    env.process(users())
    env.process(history())
    env.process(live())
