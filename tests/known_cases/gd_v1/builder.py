"""GD-V1: A callback added to an event.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="V", factor=0, random_seed=42)
    app.add_variable("seen", 0)
    app.add_signal("ev", once=True, on=[{"path": "V.variables.seen", "value": 1}])
    app.add_initial_arrival("a", size, spawn="fire_it")

    @app.task(
        name="fire_it", service_time=2, on=[{"fire": {"signal": "ev", "value": 7}}]
    )
    def fire_it(task_id):
        return {"event_type": "fired"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
