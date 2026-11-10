"""P59: Split into a drawn number of parts.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_initial_arrival("orders", size, spawn="split", attributes={"n": 3})

    @app.task(name="split", spawn={"task": "item", "count": "attr.n"})
    def split(task_id):
        return {"e": 1}

    @app.task(name="item")
    def item(task_id):
        return {"i": 1}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=10)
