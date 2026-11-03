"""GD-E2: Run until a process ends and return its value.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="E", factor=0, random_seed=42)
    app.add_signal("done", once=True)
    app.add_initial_arrival("a", size, spawn="my_proc")

    @app.task(name="my_proc", service_time=1, signal="done")
    def my_proc(task_id):
        return {"text": "Monty Python’s Flying Circus"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(stop={"signal": "done"})
