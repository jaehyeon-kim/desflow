"""P06: A piecewise arrival rate.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_arrival("o", "constant", value=1.3, spawn="t")

    @app.task(name="t")
    def t(task_id):
        return {"event_type": "arrived"}

    app.add_scenario(
        [
            {"at": 2.6, "path": "P.arrival.o.value", "value": 0.5},
            {"at": 5, "path": "P.arrival.o.value", "value": 2},
        ]
    )
    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=15)
