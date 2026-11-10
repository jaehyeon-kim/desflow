"""P66: Stop after a number of departures.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_signal("left", once=False)
    app.add_service("s", dist="constant", value=1.5)
    app.add_arrival("orders", "constant", value=1, spawn="t")

    @app.task(name="t", service_id="s", resource_id="m", signal="left")
    def t(task_id):
        return {"e": 1}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=100, stop={"condition": "signal.left.count >= 3"})
