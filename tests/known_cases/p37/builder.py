"""P37: Preemption with resume or restart.

Builder form of `model.yaml`.
"""

from pathlib import Path

from desflow import SimulationContext

HERE = Path(__file__).parent


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1, queue="priority", preempt=True)
    app.add_resource("m2", current_cap=1, queue="priority", preempt=True)
    app.add_service("s", dist="constant", value=1.5)
    app.add_arrival_replay(
        "r", HERE / "p37.csv", time="time", unit="s", fields=["pr"], spawn="resume_job"
    )
    app.add_arrival_replay(
        "q", HERE / "p37.csv", time="time", unit="s", fields=["pr"], spawn="restart_job"
    )

    @app.task(
        name="resume_job",
        service_id="s",
        resource_id="m",
        priority="attr.pr",
        on_interrupt="resume",
    )
    def resume_job(task_id):
        return {"event_type": "done"}

    @app.task(
        name="restart_job",
        service_id="s",
        resource_id="m2",
        priority="attr.pr",
        on_interrupt="restart",
    )
    def restart_job(task_id):
        return {"event_type": "done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
