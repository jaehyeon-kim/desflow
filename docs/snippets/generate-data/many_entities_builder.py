"""Many entities: ten tills, each with its own key, builder API.

The builder version of `many_entities.yaml`: ten tills, each with its own key, report
their takings every minute. Each till is its own arrival, because `add_arrival` has no
`count`.

Needs: nothing.
Run it with: python docs/snippets/generate-data/many_entities_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 300


def build() -> SimulationContext:
    """Builds the model."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    # A blueprint's `count: 10` is ten arrivals here, each with the key of its till
    for number in range(1, 11):
        app.add_arrival(
            f"tills{number}",
            "constant",
            value=60,
            spawn="report",
            key=lambda n, number=number: f"till-{number}",
        )
    report_payload = Template(
        {
            "till": {"gen": "entity"},
            "takings": {"gen": "normal", "mean": 400, "std": 60, "decimals": 2},
        },
        name="tasks.report",
    )

    @app.task(name="report")
    def report(task_id):
        """Gives one report record."""
        return report_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
