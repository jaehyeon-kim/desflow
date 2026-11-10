"""CV-087: Remove an item by value.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_store("q", items=["a", "b"])
    app.add_initial_arrival("a", size, spawn="take")

    @app.task(name="take", get={"store": "q", "filter": "item == 'b'", "wait": False})
    def take(task_id):
        return {"event_type": "removed"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
