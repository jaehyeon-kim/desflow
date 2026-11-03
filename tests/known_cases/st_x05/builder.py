"""ST-X05: A failed event handled by a process.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_signal("ev", once=True)
    app.add_initial_arrival("w", size, spawn="pem")
    app.add_initial_arrival("f", 1, spawn="fail_it")

    @app.task(name="pem", wait={"signal": "ev"}, on_fail="caught")
    def pem(task_id):
        return {"event_type": "should_not_finish"}

    @app.task(name="caught")
    def caught(task_id):
        return {"event_type": "caught"}

    @app.task(
        name="fail_it", on=[{"fire": {"signal": "ev", "fail": True, "value": ""}}]
    )
    def fail_it(task_id):
        return {"event_type": "fail"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=1, on_unhandled_failure="stop")
