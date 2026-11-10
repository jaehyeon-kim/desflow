"""The shop as a discrete-event model in desflow, run in real time into PostgreSQL.

Visitors arrive and browse, and some buy. Each order waits for a picker, ships and
completes, or is cancelled when no picker comes in time. Every rate, time and chance is
a registry parameter, so `sales.simulation.control` can change it while the model runs.
"""

import argparse
import asyncio
import logging
from datetime import UTC, datetime, timedelta

from desflow import PostgresEgress, PostgresIngress, SimulationContext

from sales.core.config import (
    ARRIVALS,
    CHANCES,
    DSN,
    MAX_PICKERS,
    PARAMS_TABLE,
    PICKERS,
    SCHEMA,
    SERVICES,
    SIM_ID,
)
from sales.core.models import User
from sales.simulation.catalogue import products
from sales.simulation.shop import basket, new_user, with_status
from sales.stores import postgres


def _wait(app: SimulationContext, service: str):
    """Returns a timeout drawn from a service's live distribution."""
    config = app.env.registry.get_config(f"{SIM_ID}.service.{service}")
    return app.env.timeout(app.sampler.sample(config))


def _chance(app: SimulationContext, name: str) -> bool:
    """Returns True with the probability of a chance variable's live value."""
    return (
        app.sampler.rng.random()
        < app.env.registry.get(f"{SIM_ID}.variables.{name}").value
    )


def build(seed: int | None = None, start: datetime | None = None) -> SimulationContext:
    """
    Builds the shop's model: its parameters, its pickers and its processes.

    Args:
        seed (int, optional): The seed for every random choice. None varies them.
        start (datetime, optional): The simulated start time, in UTC. Defaults to now.

    Returns:
        SimulationContext: The model, ready for egress, ingress and `run`.
    """
    start = start or datetime.now(UTC)
    app = SimulationContext(
        SIM_ID,
        factor=1.0,
        random_seed=seed,
        logical_start_time=start.replace(tzinfo=None),
    )
    for name, rate in ARRIVALS.items():
        app.add_arrival(name, dist="exponential", rate=rate)
    for name, (mean, std) in SERVICES.items():
        app.add_service(name, dist="lognormal", mean=mean, std=std)
    app.add_resource("pickers", current_cap=PICKERS, max_cap=MAX_PICKERS)
    for name, chance in CHANCES.items():
        app.add_variable(name, chance)
    catalogue = products()
    users: list[User] = []

    def now() -> str:
        """Returns the simulated time, in UTC, as ISO 8601 text."""
        return (start + timedelta(seconds=app.env.now)).isoformat()

    def publish(rows) -> None:
        """Writes rows to their tables."""
        for row in rows:
            app.env.publish_event(row.table, row.model_dump())

    @app.arrival_loop("visitor")
    def visitors(ctx):
        """Writes the catalogue, then starts a visit at each visitor arrival."""
        publish(catalogue)
        while True:
            yield ctx.wait_for_arrival("visitor")
            ctx.spawn(visit())

    def visit():
        """One visitor: a few page views, then possibly an order."""
        rng = app.sampler.rng
        user = (
            users[int(rng.integers(len(users)))]
            if users and _chance(app, "returning")
            else None
        )
        for _ in range(int(rng.integers(2, 6))):
            yield _wait(app, "page_view")
        if not _chance(app, "buy"):
            return
        if user is None:
            user = new_user(rng, now())
            users.append(user)
            publish([user])
        order, items = basket(rng, user, catalogue, now())
        publish([order, *items])
        app.spawn(fulfil([order, *items]))

    def fulfil(rows):
        """One order: wait for a picker or give up, then pack, ship and complete."""
        with app.get_resource("pickers").request() as picker:
            waited = yield picker | _wait(app, "patience")
            if picker not in waited:
                publish(with_status(rows, "Cancelled"))
                return
            yield _wait(app, "pick")
        publish(with_status(rows, "Shipped"))
        yield _wait(app, "transit")
        publish(with_status(rows, "Complete"))
        if _chance(app, "return"):
            yield _wait(app, "return_after")
            publish(with_status(rows, "Returned"))

    return app


def run(minutes: float | None = None, seed: int | None = None) -> None:
    """
    Runs the shop in real time, writing every row to PostgreSQL.

    Args:
        minutes (float, optional): How long to run. None runs until interrupted.
        seed (int, optional): The seed for every random choice.
    """
    asyncio.run(postgres.create_tables())
    app = build(seed)
    app.add_ingress(PostgresIngress(DSN, table_name=PARAMS_TABLE))
    app.with_batching(batch_size=4, flush_interval=1.0)  # a few rows arrive a second
    for table in postgres.TABLES:
        # Bare table names, found through the connection's search path.
        egress = PostgresEgress(
            DSN,
            table_name=table,
            upsert_keys=postgres.UPSERT_KEYS.get(table),
            server_settings={"search_path": SCHEMA},
        )
        app.add_egress(egress, when=lambda r, t=table: r.get("key") == t)
    app.run(until=minutes * 60 if minutes else None)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="Runs the shop in real time.")
    parser.add_argument(
        "--minutes", type=float, help="how long to run (default: forever)"
    )
    parser.add_argument("--seed", type=int, help="seed for the random choices")
    args = parser.parse_args()
    run(args.minutes, args.seed)
