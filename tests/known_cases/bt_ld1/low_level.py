"""BT-LD1 to BT-LD17: live-dashboard.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The specs of the generators are those of `builder.py`; the
payload of a task that draws is rendered by a `Template` named for the task, with a
`ValueRuntime` of the document's seed, and the generators of a `set` and of a wait are
bound to the places the document gives them, so every draw is from the random sequence
the document's generator at its place draws from. The catalogue and the users are kept
for the `lookup`. The pickers are a `DynamicResource`, so the scenario's change of their
number is the registry's, and an order waits for one for its patience, as the document's
`patience` and `on_renege` say. The scenario is a process that changes the registry at
the times of the document's steps. The sinks are the recording egress of the test.
"""

from datetime import datetime, timezone
from typing import Any

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)
from desflow.builder.flow import Router
from desflow.values import Template, ValueRuntime
from desflow.values.template import bind

from tests.known_cases.bt_ld1.builder import (
    ARRIVALS,
    RESOURCES,
    SCENARIO,
    SERVICES,
    TASKS,
    VARIABLES,
)
from tests.known_cases.bt_support import Entity, Run, deep

UNTIL = 300
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}
PARAMS = SimParameter(
    sim_id="sales",
    arrival={"visitor": DistributionConfig(dist="exponential", rate=4)},
    service={name: DistributionConfig(**spec) for name, spec in SERVICES.items()},
    resources={"pickers": CapacityConfig(**RESOURCES["pickers"])},
    variables=dict(VARIABLES),
)
HIDDEN = {  # what a task's `set` and `next` read from its vars, given as payload fields
    "order": {"__ids": "ids"},
    "items_complete": {"__sent_back": "sent_back"},
}
HISTORY = 1_000_000
WEIGHTS = {  # the document's routes by weights, as the engine reads them
    "catalogue_check": TASKS["catalogue_check"]["next"],
    "buy_choice": TASKS["buy_choice"]["next"],
}


def build(env: DynamicRealtimeEnvironment) -> None:
    sampler = Sampler(seed=42)
    context = Run("sales")
    values = ValueRuntime(42, env, sampler, context, stop=context._stop_run)
    context.values = values
    templates = {
        name: Template(
            {
                **deep(spec.get("payload") or {}),
                **{
                    field: {"gen": "var", "name": var}
                    for field, var in HIDDEN.get(name, {}).items()
                },
            },
            name=f"tasks.{name}",
            vars=deep(spec.get("vars")),
        )
        for name, spec in TASKS.items()
        if spec.get("payload") is not None
        and (name in HIDDEN or name in ("product_row", "new_user"))
    }
    for name in ("new_user", "product_row"):
        values.keep_rows(name, HISTORY)
    pages = deep(ARRIVALS["visitor"]["attributes"]["pages"])
    bind(pages, ("arrivals", "visitor", "attributes", "pages"))
    view = deep(TASKS["pages"]["service_time"])
    bind(view, ("tasks", "pages", "service_time"))
    patience = deep(TASKS["pick"]["patience"])
    bind(patience, ("tasks", "pick", "patience"))
    user = deep(TASKS["pick_user"]["set"]["user"])
    bind(user, ("tasks", "pick_user", "set", "user"))
    chosen = deep(TASKS["order"]["set"]["products"])
    bind(chosen, ("tasks", "order", "set", "products"))
    routers = {
        name: Router(rules, rng=sampler.generator(f"route.{name}"))
        for name, rules in WEIGHTS.items()
    }
    pickers = DynamicResource(env, "sales", "pickers")
    counts = {name: 0 for name in TASKS}
    made = {"order": 0}

    def publish(key: str, task: str, payload: dict[str, Any]) -> None:
        env.publish_event(key, payload, task=task, instance=counts[task])
        counts[task] += 1

    def render(
        name: str, entity: Entity, emit: bool = True
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Renders the payload of a task that draws, and publishes it unless `emit` is
        false (the task publishes after its wait). It returns the payload and the
        fields a later step reads from the task's vars."""
        payload = templates[name].render(context, entity)
        hidden = {field: payload.pop(field) for field in HIDDEN.get(name, {})}
        if name in ("new_user", "product_row"):
            values.record(name, payload, entity)
        if emit:
            publish(entity.key, name, payload)
        return payload, hidden

    def service(name: str) -> Any:
        return env.timeout(
            sampler.sample(env.registry.get_config(f"sales.service.{name}"))
        )

    def row(entity: Entity, task: str, status: str) -> None:
        a = entity.attributes
        publish(
            entity.key,
            task,
            {
                "__table__": "orders",
                "id": a["order_id"],
                "user_id": a["user"],
                "status": status,
                "num_of_item": a["n"],
                "created_at": a["created_at"],
            },
        )

    def items(entity: Entity, task: str, status: str) -> None:
        """The wait of the document: one item row for each item, then the task's event."""
        a = entity.attributes
        for i in range(a["n"]):
            product = a["products"][i]
            publish(
                entity.key,
                "order_item",
                {
                    "__table__": "order_items",
                    "id": a["iids"][i],
                    "order_id": a["order_id"],
                    "user_id": a["user"],
                    "product_id": product["id"],
                    "status": status,
                    "sale_price": product["retail_price"],
                    "created_at": a["created_at"],
                },
            )
        publish(entity.key, task, deep(TASKS[task]["payload"]))

    def visit(entity: Entity) -> Any:
        a = entity.attributes
        publish(entity.key, "catalogue_check", {"event_type": "visit"})
        if routers["catalogue_check"].choose(None, env=env) == "pick_user":
            a["user"] = values.evaluate(user, values.scope_of(entity))
        else:
            a["user"] = ""
        while True:
            yield env.timeout(values.evaluate(view, values.scope_of(entity)))
            a["n"] += 1
            if not a["n"] < a["pages"]:
                break
        publish(entity.key, "buy_choice", {"event_type": "pages_done"})
        if routers["buy_choice"].choose(None, env=env) == "walk_away":
            publish(entity.key, "walk_away", {"event_type": "left"})
            return
        if a["user"] != "":
            publish(entity.key, "returning", {"event_type": "returning"})
        else:
            env.registry.update(
                "sales.variables.users",
                env.registry.get("sales.variables.users").value + 1,
            )
            payload, _ = render("new_user", entity)
            a["user"] = payload["id"]
        payload, hidden = render("order", entity)
        made["order"] += 1
        a.update(
            order_id=payload["id"],
            created_at=payload["created_at"],
            n=payload["num_of_item"],
            iids=hidden["__ids"],
        )
        a["products"] = values.evaluate(
            chosen, values.scope_of(entity, names={"n": a["n"]})
        )
        items(entity, "items", "Processing")
        yield env.process(fulfil(entity))

    def fulfil(entity: Entity) -> Any:
        waited = values.evaluate(patience, values.scope_of(entity))
        with pickers.request() as picker:
            got = yield picker | env.timeout(waited)
            if picker not in got:
                publish(entity.key, "pick", {"status": "reneged"})
                row(entity, "cancelled", "Cancelled")
                items(entity, "items_cancelled", "Cancelled")
                return
            yield service("pick")
        row(entity, "pick", "Shipped")
        items(entity, "items_shipped", "Shipped")
        yield service("transit")
        row(entity, "transit", "Complete")
        _, hidden = render("items_complete", entity, emit=False)
        items(entity, "items_complete", "Complete")
        if hidden["__sent_back"]:
            yield service("return_after")
            row(entity, "returned", "Returned")
            items(entity, "items_returned", "Returned")

    def catalogue() -> Any:
        for number in range(1, ARRIVALS["catalogue"]["initial"] + 1):
            render("product_row", Entity(f"catalogue-{number}"))
        yield env.timeout(0)

    def visitors() -> Any:
        config = env.registry.get_config("sales.arrival.visitor")
        config.stream = "arrival.visitor"
        number = 0
        while True:
            yield env.timeout(sampler.sample(config))
            number += 1
            entity = Entity(f"visitor-{number}", n=0, pages=0)
            entity.attributes["pages"] = values.evaluate(pages, values.scope_of())
            env.process(visit(entity))

    def scenario() -> Any:
        at = 0.0
        for step in SCENARIO:
            yield env.timeout(step["at"] - at)
            at = step["at"]
            env.registry.update(step["path"], step["value"])

    env.process(catalogue())
    env.process(visitors())
    env.process(scenario())
