"""A film rental store: films, customers, rentals, payments and returns, builder API.

The builder version of `film_rental_store.yaml`: 20 films and 15 customers at the start,
a rental about every 8 minutes for a week, paid at once and returned 1 to 7 days later.

Needs: the fake extra.
Run it with: python examples/gallery/film_rental_store_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template, generator

UNTIL = 7 * 86400


def build() -> SimulationContext:
    """Builds the store."""
    app = SimulationContext(
        sim_id="Store",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.add_initial_arrival("films", 20, spawn="film")
    app.add_initial_arrival("customers", 15, spawn="customer")
    app.add_arrival("rentals", "exponential", rate=0.002, spawn="rental")
    film_payload = Template(
        {
            "film_id": {"gen": "sequence", "start": 1},
            "title": {
                "gen": "format",
                "template": "{adjective} {noun}",
                "names": {
                    "adjective": {
                        "gen": "one_of",
                        "choices": ["Silent", "Broken", "Golden", "Hidden", "Last"],
                    },
                    "noun": {
                        "gen": "one_of",
                        "choices": ["River", "Garden", "Highway", "Letter", "Harbour"],
                    },
                },
            },
            "rating": {
                "gen": "weighted",
                "options": {"G": 2, "PG": 3, "PG-13": 3, "R": 3, "NC-17": 1},
            },
            "rental_rate": {"gen": "one_of", "choices": [0.99, 2.99, 4.99]},
            "length": {"gen": "integer", "bounds": [60, 180]},
        },
        name="tasks.film",
    )
    customer_payload = Template(
        {
            "customer_id": {"gen": "sequence", "start": 1},
            "first_name": {"gen": "fake", "method": "first_name"},
            "last_name": {"gen": "fake", "method": "last_name"},
            "active": {"gen": "boolean", "p": 0.9},
        },
        name="tasks.customer",
    )
    rental_payload = Template(
        {
            "rental_id": {"gen": "sequence", "start": 1},
            "rental_date": {"gen": "now"},
            "film_id": {"gen": "var", "name": "film", "path": "film_id"},
            "rental_rate": {"gen": "var", "name": "film", "path": "rental_rate"},
            "customer_id": {"gen": "lookup", "task": "customer", "path": "customer_id"},
        },
        name="tasks.rental",
        vars={"film": {"gen": "lookup", "task": "film"}},
    )
    payment_payload = Template(
        {
            "rental_id": {"gen": "upstream", "path": "rental_id"},
            "customer_id": {"gen": "upstream", "path": "customer_id"},
            "amount": {"gen": "upstream", "path": "rental_rate"},
            "payment_date": {"gen": "now"},
        },
        name="tasks.payment",
    )
    return_payload = Template(
        {
            "rental_id": {"gen": "upstream", "path": "rental_id"},
            "return_date": {"gen": "now"},
        },
        name="tasks.return",
    )

    @app.task(name="film", history=1_000_000)
    def film(task_id):
        """Gives one film."""
        return film_payload

    @app.task(name="customer", history=1_000_000)
    def customer(task_id):
        """Gives one customer."""
        return customer_payload

    @app.task(name="rental", next="payment")
    def rental(task_id):
        """Starts a rental of a film by a customer."""
        return rental_payload

    @app.task(name="payment", next="return")
    def payment(task_id):
        """Pays for the rental at its rate."""
        return payment_payload

    # 1 to 7 days, in seconds
    @app.task(
        name="return",
        wait={"timeout": generator({"gen": "uniform", "low": 86400, "high": 604800})},
    )
    def return_(task_id):
        """Returns the film."""
        return return_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
