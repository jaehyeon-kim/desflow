"""Urgent support tickets are served before older tickets of lower priority.

The Builder form of urgent_tickets_are_served_first.yaml, with the same records. One
agent solves a ticket in about 50 seconds, and the agent's queue is ordered by the
priority each ticket arrives with, so a ticket of priority 1 waits less than one of
priority 3.

Needs: nothing
Run it with: python examples/gallery/urgent_tickets_are_served_first_builder.py
"""

import logging
from datetime import datetime

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template, generator

UNTIL = 14400


def build() -> SimulationContext:
    """Builds the model."""
    app = (
        SimulationContext(
            sim_id="Support",
            factor=0,
            random_seed=42,
            logical_start_time=datetime(2026, 1, 1),
        )
        .add_egress(ConsoleEgress())
        .add_resource("agents", current_cap=1, max_cap=1, queue="priority")
        .add_service("solve", dist="normal", mean=50, std=10)
        .add_arrival(
            "tickets",
            "exponential",
            rate=0.018,
            attributes={
                "priority": generator(
                    {"gen": "weighted", "options": {1: 10, 2: 30, 3: 60}}
                )
            },
            spawn="solve_ticket",
        )
    )

    solved_payload = Template(
        {"event_type": "solved", "priority": {"gen": "attr", "name": "priority"}},
        name="tasks.solve_ticket",
    )

    @app.task(
        name="solve_ticket",
        service_id="solve",
        resource_id="agents",
        priority="attr.priority",
    )
    def solve_ticket(task_id):
        return solved_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
