"""CV-114: Raise again after cleanup.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="body")

    @app.task(name="body", fail="True", fail_reason="body failed", on_fail="cleanup")
    def body(task_id):
        return {"event_type": "body"}

    @app.task(name="cleanup", fail="upstream.status == 'failed'", fail_reason="again")
    def cleanup(task_id):
        return {"event_type": "cleanup"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
