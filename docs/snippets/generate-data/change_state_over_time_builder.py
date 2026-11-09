"""Change state over time: stations move between three states, builder API.

The builder version of `change_state_over_time.yaml`: one task for each state, with a
dwell time as `service_time` and the next state in `next`.

Needs: nothing.
Run it with: python docs/snippets/generate-data/change_state_over_time_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 7200


def build() -> SimulationContext:
    """Builds the three stations."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    # --8<-- [start:arrival]
    app.add_initial_arrival(
        "stations", 3, spawn="running", key=lambda n: f"station-{n}"
    )
    # --8<-- [end:arrival]

    # --8<-- [start:payload]
    def state(name):
        return Template(
            {"station": {"gen": "entity"}, "state": name}, name=f"tasks.{name}"
        )

    running, slow, stopped = state("running"), state("slow"), state("stopped")

    @app.task(
        name="running",
        service_time=300,
        next={"weights": {"running": 85, "slow": 15}},
    )
    def running_task(task_id):
        """The station is running."""
        return running

    @app.task(
        name="slow",
        service_time=120,
        next={"weights": {"running": 55, "slow": 25, "stopped": 20}},
    )
    def slow_task(task_id):
        """The station is slow."""
        return slow

    @app.task(name="stopped", service_time=240, next="slow")
    def stopped_task(task_id):
        """The station is stopped."""
        return stopped

    # --8<-- [end:payload]

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
