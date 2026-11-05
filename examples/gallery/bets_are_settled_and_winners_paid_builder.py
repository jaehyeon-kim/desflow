"""Each bet is settled after the match, and a winner is paid the stake times the odds.

The Builder form of bets_are_settled_and_winners_paid.yaml, with the same records. A bet
is placed with a stake and odds. One to two minutes later it is settled: 45 percent win
and are paid the stake times the odds, and the rest lose and are paid nothing.

Needs: nothing
Run it with: python examples/gallery/bets_are_settled_and_winners_paid_builder.py
"""

import logging
from datetime import datetime

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template, generator

UNTIL = 1200


def build() -> SimulationContext:
    """Builds the model."""
    app = (
        SimulationContext(
            sim_id="Book",
            factor=0,
            random_seed=42,
            logical_start_time=datetime(2026, 1, 1),
        )
        .add_egress(ConsoleEgress())
        .add_arrival("bets", "exponential", rate=0.2, spawn="place_bet")
    )

    place_payload = Template(
        {
            "bet_id": {"gen": "sequence", "start": 1},
            "stake": {
                "gen": "weighted",
                "options": {5: 50, 10: 30, 50: 15, 100: 5},
            },
            "odds": {"gen": "uniform", "low": 1.5, "high": 4, "decimals": 2},
        },
        name="tasks.place_bet",
    )
    settle_payload = Template(
        {
            "bet_id": {"gen": "upstream", "path": "bet_id"},
            "result": {
                "gen": "when",
                "condition": "won",
                "then": "won",
                "else": "lost",
            },
            "payout": {
                "gen": "when",
                "condition": "won",
                "then": {
                    "gen": "multiply",
                    "args": [
                        {"gen": "upstream", "path": "stake"},
                        {"gen": "upstream", "path": "odds"},
                    ],
                    "decimals": 2,
                },
                "else": 0,
            },
        },
        name="tasks.settle_bet",
    )

    @app.task(name="place_bet", next="settle_bet")
    def place_bet(task_id):
        return place_payload

    @app.task(
        name="settle_bet",
        vars={"won": generator({"gen": "boolean", "p": 0.45})},
        service_time=generator({"gen": "uniform", "low": 60, "high": 120}),
    )
    def settle_bet(task_id):
        return settle_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
