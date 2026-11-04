"""GA-7: A day of history is written in seconds, then events continue in real time.

Builder form of `model.yaml`.
"""

from datetime import datetime, timedelta, timezone

from desflow import SimulationContext

NOW = datetime.now(timezone.utc)


def build() -> SimulationContext:
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=NOW - timedelta(days=1),
        go_live_at=NOW,
    )
    app.add_arrival("visits", "constant", value=1, spawn="visit")

    @app.task(name="visit")
    def visit(task_id):
        return {"event_type": "visit"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=86403)
