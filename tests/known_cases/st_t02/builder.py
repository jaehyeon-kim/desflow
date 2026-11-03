"""ST-T02: A negative delay fails when the run reaches it.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_variable("delay", -1)
    app.add_initial_arrival("a", size, spawn="pem")

    @app.task(name="pem", wait={"timeout": "registry['T.variables.delay']"})
    def pem(task_id):
        return {"event_type": "unreachable"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
