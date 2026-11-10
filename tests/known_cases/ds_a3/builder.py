"""DS-A3: A group size drawn for each arrival.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="A3", factor=0, random_seed=42)
    app.add_arrival(
        "a",
        "constant",
        value=5,
        group=generator({"gen": "sample", "dist": "poisson", "mean": 2}),
        spawn="member",
    )

    @app.task(name="member")
    def member(task_id):
        return {"event_type": "member"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=16)
