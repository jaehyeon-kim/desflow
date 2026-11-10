"""Visitors drop out at each step of a signup funnel.

The Builder form of visitors_drop_out_at_each_signup_step.yaml, with the same records.
Of every 100 visitors, 40 sign up, 20 of those start a trial, and 5 of those pay. A
visitor who drops out ends in one `left` task, whose record names the step the visitor
left after.

Needs: nothing
Run it with: python examples/gallery/visitors_drop_out_at_each_signup_step_builder.py
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
            sim_id="Funnel",
            factor=0,
            random_seed=42,
            logical_start_time=datetime(2026, 1, 1),
        )
        .add_egress(ConsoleEgress())
        .add_arrival("visitors", "exponential", rate=0.5, spawn="visited")
    )

    left_payload = Template(
        {"stage": "left", "after": {"gen": "upstream", "path": "stage"}},
        name="tasks.left",
    )

    @app.task(name="visited", next={"weights": {"signed_up": 40, "left": 60}})
    def visited(task_id):
        return {"stage": "visited"}

    @app.task(
        name="signed_up",
        service_time=generator({"gen": "uniform", "low": 5, "high": 30}),
        next={"weights": {"trial_started": 50, "left": 50}},
    )
    def signed_up(task_id):
        return {"stage": "signed_up"}

    @app.task(
        name="trial_started",
        service_time=generator({"gen": "uniform", "low": 10, "high": 60}),
        next={"weights": {"paid": 25, "left": 75}},
    )
    def trial_started(task_id):
        return {"stage": "trial_started"}

    @app.task(name="paid")
    def paid(task_id):
        return {"stage": "paid"}

    @app.task(name="left")
    def left(task_id):
        return left_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
