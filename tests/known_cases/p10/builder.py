"""P10: Change a source from a flow.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_service("s", dist="constant", value=4)
    app.add_arrival("o", "constant", value=2, spawn="k")

    @app.task(name="k", next="w")
    def k(task_id):
        return {"event_type": "arrived"}

    @app.task(name="w", service_id="s", on=[{"path": "P.arrival.o.value", "value": 3}])
    def w(task_id):
        return {"event_type": "changed"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=14)
