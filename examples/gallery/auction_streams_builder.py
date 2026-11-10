"""Auction streams: people, auctions and bids, builder API.

The builder version of `auction_streams.yaml`: about 1 new person for 3 new auctions and
46 bids, with ten people and five auctions at the start so that every lookup finds a
row.

Needs: the fake extra.
Run it with: python examples/gallery/auction_streams_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 600


def build() -> SimulationContext:
    """Builds the auction house."""
    app = SimulationContext(
        sim_id="Auction",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.add_arrival("people", "exponential", rate=0.02, spawn="person", initial=10)
    app.add_arrival("auctions", "exponential", rate=0.06, spawn="auction", initial=5)
    app.add_arrival("bids", "exponential", rate=0.92, spawn="bid")
    person_payload = Template(
        {
            "person_id": {"gen": "sequence", "start": 1000},
            "name": {"gen": "fake", "method": "name"},
            "city": {
                "gen": "one_of",
                "choices": ["Phoenix", "Boise", "Cheyenne", "Portland", "Kent"],
            },
            "state": {
                "gen": "weighted",
                "options": {"AZ": 3, "ID": 2, "WY": 1, "OR": 3, "WA": 3},
            },
        },
        name="tasks.person",
    )
    auction_payload = Template(
        {
            "auction_id": {"gen": "sequence", "start": 1},
            "seller": {"gen": "lookup", "task": "person", "path": "person_id"},
            "category": {"gen": "integer", "bounds": [1, 10]},
            "reserve": {"gen": "integer", "bounds": [100, 1000]},
            "expires": {"gen": "now", "offset": "1 hour"},
        },
        name="tasks.auction",
    )
    bid_payload = Template(
        {
            "auction": {"gen": "lookup", "task": "auction", "path": "auction_id"},
            "bidder": {"gen": "lookup", "task": "person", "path": "person_id"},
            "price": {"gen": "integer", "bounds": [100, 2000]},
            "bid_time": {"gen": "now"},
        },
        name="tasks.bid",
    )

    # A task that something looks up keeps its finished rows
    @app.task(name="person", history=1_000_000)
    def person(task_id):
        """Gives one person."""
        return person_payload

    @app.task(name="auction", history=1_000_000)
    def auction(task_id):
        """Gives one auction."""
        return auction_payload

    @app.task(name="bid")
    def bid(task_id):
        """Gives one bid."""
        return bid_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
