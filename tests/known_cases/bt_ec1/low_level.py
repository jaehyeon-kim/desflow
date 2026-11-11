"""BT-EC1 to BT-EC9: ecommerce-cdc.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The specs of the generators are those of `builder.py`; each
payload is rendered by a `Template` named for the task, with a `ValueRuntime` of the
document's seed, so every field draws from the random sequence the document's generator
at its place draws from. The users are kept for the `lookup` of the moves and the orders.
The warehouse is a SimPy `Resource` of three pickers, and an order waits for it for its
patience, as the document's `patience` and `on_renege` say.
"""

from datetime import datetime, timezone
from typing import Any

import simpy
from desflow import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
    SimParameter,
)
from desflow.builder.flow import Router
from desflow.values import Template, ValueRuntime
from desflow.values.template import bind

from tests.known_cases.bt_ec1.builder import ARRIVALS, SERVICES, TASKS, VARIABLES
from tests.known_cases.bt_support import Entity, Run, deep

UNTIL = 1200
START = datetime(2026, 9, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}
PARAMS = SimParameter(
    sim_id="ecommerce",
    arrival={
        "visitor": DistributionConfig(dist="exponential", rate=1.0),
        "move": DistributionConfig(dist="exponential", rate=0.05),
    },
    service={name: DistributionConfig(**spec) for name, spec in SERVICES.items()},
    variables=dict(VARIABLES),
)
HIDDEN = {  # what a task's `set` and `next` read from its vars, given as payload fields
    "session_start": {"__sid": "session_id"},
    "browse": {"__s": "s"},
    "cart": {"__s": "s"},
    "purchase": {"__s": "s"},
    "order_row": {"__oid": "oid", "__created": "created", "__uid": "uid", "__n": "n"},
    "deliver_order": {"__sent_back": "sent_back"},
}


def build(env: DynamicRealtimeEnvironment, size: int = 10) -> None:
    sampler = Sampler(seed=42)
    context = Run("ecommerce")
    values = ValueRuntime(42, env, sampler, context, stop=context._stop_run)
    context.values = values
    templates = {
        name: Template(
            {
                **deep(spec.get("payload", {})),
                **{
                    field: {"gen": "var", "name": var}
                    for field, var in HIDDEN.get(name, {}).items()
                },
            },
            name=f"tasks.{name}",
            vars=deep(spec.get("vars")),
            vars_once=deep(spec.get("vars_once")),
        )
        for name, spec in TASKS.items()
    }
    values.keep_rows("new_user", 1_000_000)
    pages = deep(ARRIVALS["visitor"]["attributes"]["pages"])
    bind(pages, ("arrivals", "visitor", "attributes", "pages"))
    times = {}
    for name in ("browse", "cart", "purchase"):
        times[name] = deep(TASKS[name]["service_time"])
        bind(times[name], ("tasks", name, "service_time"))
    patience = deep(TASKS["pick_order"]["patience"])
    bind(patience, ("tasks", "pick_order", "patience"))
    router = Router(
        {"weights": {"cart": VARIABLES["buy"], "leave": 1 - VARIABLES["buy"]}},
        rng=sampler.generator("route.buy_choice"),
    )
    pickers = simpy.Resource(env, capacity=3)
    counts = {name: 0 for name in TASKS}
    spawned = {"order_row": 0}

    def publish(name: str, entity: Entity) -> tuple[dict[str, Any], dict[str, Any]]:
        """Renders the task's payload for the entity and publishes it. It returns the
        payload and the fields a later step reads from the task's vars."""
        payload = templates[name].render(context, entity)
        hidden = {field: payload.pop(field) for field in HIDDEN.get(name, {})}
        values.record(name, payload, entity)
        env.publish_event(entity.key, payload, task=name, instance=counts[name])
        counts[name] += 1
        return payload, hidden

    def service(config: str, entity: Entity) -> Any:
        return env.timeout(
            sampler.sample(env.registry.get_config(f"ecommerce.service.{config}"))
        )

    def wait_for(name: str, entity: Entity) -> Any:
        return env.timeout(values.evaluate(times[name], values.scope_of(entity)))

    def session(entity: Entity) -> Any:
        _, hidden = publish("session_start", entity)
        entity.attributes.update(seq=1, sid=hidden["__sid"])
        while True:
            yield wait_for("browse", entity)
            _, hidden = publish("browse", entity)
            entity.attributes.update(n=entity.attributes["n"] + 1, seq=hidden["__s"])
            if not entity.attributes["n"] < entity.attributes["pages"]:
                break
        publish("buy_choice", entity)
        if router.choose(None) == "leave":
            publish("leave", entity)
            return
        yield wait_for("cart", entity)
        _, hidden = publish("cart", entity)
        entity.attributes.update(seq=hidden["__s"])
        yield wait_for("purchase", entity)
        publish("purchase", entity)
        spawned["order_row"] += 1
        env.process(order(Entity(f"order_row-{spawned['order_row']}")))

    def order(entity: Entity) -> Any:
        _, hidden = publish("order_row", entity)
        entity.attributes.update(
            order_id=hidden["__oid"],
            created_at=hidden["__created"],
            user=hidden["__uid"],
            n=hidden["__n"],
        )
        waited = values.evaluate(patience, values.scope_of(entity))
        with pickers.request() as picker:
            got = yield picker | env.timeout(waited)
            if picker not in got:
                publish("cancel_order", entity)
                return
            yield service("pick", entity)
        payload, _ = publish("pick_order", entity)
        entity.attributes["shipped_at"] = payload["shipped_at"]
        yield service("transit", entity)
        payload, hidden = publish("deliver_order", entity)
        entity.attributes["delivered_at"] = payload["delivered_at"]
        if hidden["__sent_back"]:
            yield service("return_after", entity)
            publish("return_order", entity)

    def users() -> Any:
        for number in range(1, size + 1):
            publish("center", Entity(f"centers-{number}"))
        for number in range(1, ARRIVALS["users"]["initial"] + 1):
            entity = Entity(f"users-{number}")
            publish("new_user", entity)
        yield env.timeout(0)

    def visitors() -> Any:
        config = env.registry.get_config("ecommerce.arrival.visitor")
        config.stream = "arrival.visitor"
        number = 0
        while True:
            yield env.timeout(sampler.sample(config))
            number += 1
            entity = Entity(f"visitor-{number}", n=0, seq=0, sid="")
            entity.attributes["pages"] = values.evaluate(pages, values.scope_of())
            env.process(session(entity))

    def moves() -> Any:
        config = env.registry.get_config("ecommerce.arrival.move")
        config.stream = "arrival.move"
        number = 0
        while True:
            yield env.timeout(sampler.sample(config))
            number += 1
            publish("move", Entity(f"move-{number}"))

    env.process(users())
    env.process(visitors())
    env.process(moves())
