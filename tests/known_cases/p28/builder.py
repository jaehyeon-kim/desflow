"""P28: Try and catch around a step.

Builder form of `model.yaml`.
"""

from pathlib import Path

from desflow import SimulationContext

HERE = Path(__file__).parent


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_service("s", dist="constant", value=1)
    app.add_arrival_replay(
        "o", HERE / "rows.csv", time="time", unit="s", fields=["i"], spawn="k"
    )

    @app.task(
        name="k",
        service_id="s",
        resource_id="m",
        fail="attr.i % 2 == 0",
        fail_reason="bad",
        on_fail="handled",
    )
    def k(task_id):
        return {"event_type": "ok"}

    @app.task(name="handled")
    def handled(task_id):
        return {"event_type": "handled"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
