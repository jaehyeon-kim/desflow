"""P62: A gate on the state.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_variable("open", 0)
    app.add_arrival("orders", "constant", value=1, spawn="t")

    @app.task(name="t", wait={"condition": "registry['P.variables.open'] == 1"})
    def t(task_id):
        return {"e": 1}

    app.add_scenario([{"at": 3.5, "path": "P.variables.open", "value": 1}])
    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=5)
