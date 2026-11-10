"""Callers hang up when they wait for an agent for longer than they will tolerate.

The Builder form of callers_hang_up_after_waiting_too_long.yaml, with the same records.
Calls arrive at 54 an hour and two agents answer about 30 an hour, so a queue builds.
A caller who waits longer than their patience, 1 to 5 minutes, hangs up. A scenario step
adds two agents after half an hour, so calls wait less and fewer callers hang up.

Needs: nothing
Run it with: python examples/gallery/callers_hang_up_after_waiting_too_long_builder.py
"""

import logging
from datetime import datetime

from desflow import ConsoleEgress, SimulationContext
from desflow.values import generator

UNTIL = 3600


def build() -> SimulationContext:
    """Builds the model."""
    app = (
        SimulationContext(
            sim_id="Centre",
            factor=0,
            random_seed=42,
            logical_start_time=datetime(2026, 1, 1),
        )
        .add_egress(ConsoleEgress())
        .add_resource("agents", current_cap=2, max_cap=4)
        .add_service("talk", dist="normal", mean="4 min", std="1 min")
        .add_arrival("calls", "exponential", rate=0.015, spawn="answer_call")
        .add_scenario(
            [
                {
                    "at": "30 min",
                    "path": "Centre.resources.agents.current_cap",
                    "value": 4,
                }
            ]
        )
    )

    @app.task(
        name="answer_call",
        service_id="talk",
        resource_id="agents",
        patience=generator({"gen": "uniform", "low": 60, "high": 300}),
        on_renege="hang_up",
    )
    def answer_call(task_id):
        return {"event_type": "answered", "call_id": task_id}

    @app.task(name="hang_up")
    def hang_up(task_id):
        return {"event_type": "hung_up"}

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
