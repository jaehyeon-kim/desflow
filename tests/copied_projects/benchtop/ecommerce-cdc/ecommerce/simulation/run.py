"""Runs the shop in desflow and writes every row it produces to PostgreSQL.

Visitors arrive at random, and each visit is a process that views pages and may buy. Each
order is then a process of its own: it queues for a warehouse picker, is packed, shipped
and delivered, and is sometimes returned; one that waits too long is cancelled.

Run: python -m ecommerce.simulation.run --minutes 5   (default: until Ctrl + C)
"""

import argparse
import logging
from collections.abc import Generator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from desflow import (
    KafkaAdminConnector,
    KafkaIngress,
    PostgresEgress,
    SimulationContext,
)
from faker import Faker
from pydantic import BaseModel

from ecommerce.core.config import (
    ARRIVALS,
    CHANCES,
    CONTROL_TOPIC,
    DSN,
    INITIAL_USERS,
    KAFKA,
    MAX_PICKERS,
    PICKERS,
    SERVICES,
    SIM_ID,
)
from ecommerce.core.models import KEY, TABLES, Event, Order, OrderItem, Product, User
from ecommerce.simulation import shop
from ecommerce.simulation.catalogue import DIST_CENTERS, products
from ecommerce.stores import postgres

# A SimPy process: it yields events and is sent their values.
Process = Generator[Any, Any, None]


@dataclass
class Shop:
    """
    What the processes share.

    Attributes:
        start (datetime): The simulated start time, in UTC.
        fake (Faker): The seeded generator of names and addresses.
        products (list[Product]): The catalogue.
        users (list[User]): The registered users, for returning visitors and moves.
    """

    start: datetime
    fake: Faker
    products: list[Product] = field(default_factory=products)
    users: list[User] = field(default_factory=list)


def build(
    start: datetime, seed: int | None = None, factor: float = 1.0
) -> SimulationContext:
    """
    Builds the simulation: its parameters, the warehouse pickers and the processes.

    Args:
        start (datetime): The simulated start time, in UTC.
        seed (int, optional): The seed for every random choice. None varies them.
        factor (float): Simulated seconds per real second: 1.0 runs in real time, and 0
            runs as fast as possible.

    Returns:
        SimulationContext: The simulation, ready for egress and `run`.
    """
    fake = Faker()
    fake.seed_instance(seed)
    state = Shop(start, fake)
    app = SimulationContext(
        SIM_ID,
        factor=factor,
        random_seed=seed,
        logical_start_time=start.replace(tzinfo=None),
    )
    for name, rate in ARRIVALS.items():
        app.add_arrival(name, dist="exponential", rate=rate)
    for name, (mean, std) in SERVICES.items():
        app.add_service(name, dist="lognormal", mean=mean, std=std)
    for name, chance in CHANCES.items():
        app.add_variable(name, chance)
    app.add_resource("pickers", current_cap=PICKERS, max_cap=MAX_PICKERS)
    app.arrival_loop("visitor")(lambda ctx: visitors(ctx, state))
    app.arrival_loop("move")(lambda ctx: moves(ctx, state))
    return app


def visitors(ctx: SimulationContext, state: Shop) -> Process:
    """Writes the catalogue and the first users, then starts a visit at each arrival."""
    rng = ctx.sampler.rng
    state.users += [
        shop.new_user(state.fake, rng, _now(ctx, state)) for _ in range(INITIAL_USERS)
    ]
    rows: list[BaseModel] = [*DIST_CENTERS, *state.products, *state.users]
    for row in rows:
        ctx.env.publish_event(TABLES[type(row)], row.model_dump())
    while True:
        yield ctx.wait_for_arrival("visitor")
        ctx.spawn(visit(ctx, state))


def visit(ctx: SimulationContext, state: Shop) -> Process:
    """One visit: a few pages with a wait on each, and an order at the end for a buyer."""
    rng = ctx.sampler.rng
    returning = _chance(ctx, "returning")
    user = state.users[int(rng.integers(len(state.users)))] if returning else None
    buys = _chance(ctx, "buy")
    session_id = shop.new_id(rng)
    browser = str(rng.choice(["Chrome", "Safari", "Firefox"]))
    source = str(rng.choice(["Email", "Adwords", "Organic", "YouTube"]))
    where = shop.city(rng) if user is None else user.model_dump()
    for n, (page, uri) in enumerate(shop.pages(rng, state.products, buys), start=1):
        if page == "purchase":
            if user is None:  # a new buyer signs up first
                user = shop.new_user(state.fake, rng, _now(ctx, state))
                state.users.append(user)
                ctx.env.publish_event("users", user.model_dump())
            order, items = shop.new_order(rng, state.products, user, _now(ctx, state))
            _publish(ctx, order, items)
            ctx.spawn(fulfil(ctx, state, order, items))
        event = Event(
            id=shop.new_id(rng),
            user_id=user.id if user else None,
            session_id=session_id,
            sequence_number=n,
            event_type=page,
            uri=uri,
            city=where["city"],
            state=where["state"],
            postal_code=where["postal_code"],
            browser=browser,
            traffic_source=source,
            ip_address=state.fake.ipv4(),
            created_at=_now(ctx, state),
        )
        ctx.env.publish_event("events", event.model_dump())
        yield _wait(ctx, "page_view")


def fulfil(
    ctx: SimulationContext, state: Shop, order: Order, items: list[OrderItem]
) -> Process:
    """One order: packed by a picker or cancelled, then shipped, delivered and maybe returned."""
    with ctx.get_resource("pickers").request() as picker:
        got = yield picker | _wait(ctx, "patience")
        if picker not in got:
            _publish(ctx, *shop.advance(order, items, "Cancelled", _now(ctx, state)))
            return
        yield _wait(ctx, "pick")
    order, items = shop.advance(order, items, "Shipped", _now(ctx, state))
    _publish(ctx, order, items)
    yield _wait(ctx, "transit")
    order, items = shop.advance(order, items, "Delivered", _now(ctx, state))
    _publish(ctx, order, items)
    if _chance(ctx, "return"):
        yield _wait(ctx, "return_after")
        _publish(ctx, *shop.advance(order, items, "Returned", _now(ctx, state)))


def moves(ctx: SimulationContext, state: Shop) -> Process:
    """At each move arrival, a registered user moves to a new address."""
    while True:
        yield ctx.wait_for_arrival("move")
        index = int(ctx.sampler.rng.integers(len(state.users)))
        state.users[index] = shop.moved(
            state.users[index], state.fake, ctx.sampler.rng, _now(ctx, state)
        )
        ctx.env.publish_event("users", state.users[index].model_dump())


def _publish(ctx: SimulationContext, order: Order, items: list[OrderItem]) -> None:
    """Publishes an order and its items, each to its table."""
    ctx.env.publish_event("orders", order.model_dump())
    for item in items:
        ctx.env.publish_event("order_items", item.model_dump())


def _chance(ctx: SimulationContext, name: str) -> bool:
    """Returns True with the probability of a chance variable's live value."""
    return (
        ctx.sampler.rng.random()
        < ctx.env.registry.get(f"{SIM_ID}.variables.{name}").value
    )


def _wait(ctx: SimulationContext, service: str) -> Any:
    """Returns a timeout drawn from a service's live distribution in the registry."""
    config = ctx.env.registry.get_config(f"{SIM_ID}.service.{service}")
    return ctx.env.timeout(ctx.sampler.sample(config))


def _now(ctx: SimulationContext, state: Shop) -> str:
    """Returns the simulated time as ISO 8601 text in UTC."""
    return (state.start + timedelta(seconds=ctx.env.now)).isoformat(timespec="seconds")


def run(minutes: float | None = None, seed: int | None = None) -> None:
    """
    Runs the shop in real time, writing every row to its table and reading live changes.

    Args:
        minutes (float, optional): How long to run. None runs until interrupted.
        seed (int, optional): The seed for every random choice.
    """
    postgres.create_tables()
    KafkaAdminConnector(KAFKA).create_topics([{"name": CONTROL_TOPIC}])
    app = build(datetime.now(UTC), seed)
    app.add_ingress(KafkaIngress(topic=CONTROL_TOPIC, bootstrap_servers=KAFKA))
    app.with_batching(batch_size=4, flush_interval=1.0)  # a few rows a second per table
    for table in TABLES.values():
        app.add_egress(
            PostgresEgress(DSN, table_name=table, upsert_keys=KEY),
            when=lambda r, t=table: r["stream_type"] == "event" and r["key"] == t,
        )
    app.run(until=minutes * 60 if minutes else None)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)  # a line per connection
    parser = argparse.ArgumentParser(description="Runs the shop in real time.")
    parser.add_argument(
        "--minutes", type=float, help="how long to run (default: until stopped)"
    )
    parser.add_argument("--seed", type=int, help="seed for every random choice")
    args = parser.parse_args()
    run(args.minutes, args.seed)
