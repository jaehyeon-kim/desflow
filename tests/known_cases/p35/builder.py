"""P35: Change the queue limit at run time.

Builder form of `model.yaml`.
"""

from pathlib import Path

from desflow import SimulationContext

HERE = Path(__file__).parent


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1, queue_limit=1)
    app.add_service("s", dist="constant", value=5)
    app.add_arrival_replay("a", HERE / "a.csv", time="time", unit="s", spawn="job")

    @app.task(name="job", resource_id="m", service_id="s")
    def job(task_id):
        return {"event_type": "done"}

    app.add_scenario([{"at": 2, "path": "P.resources.m.queue_limit", "value": 3}])
    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=7)
