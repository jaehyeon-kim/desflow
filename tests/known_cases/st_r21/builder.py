"""ST-R21: A third put waits on a full store.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_store("s", current_cap=2)
    app.add_initial_arrival(
        "puts", 3, spawn="put_it", attributes={"i": generator({"gen": "sequence"})}
    )

    @app.task(
        name="put_it",
        put={"store": "s", "item": generator({"gen": "expr", "expr": "attr.i"})},
    )
    def put_it(task_id):
        return {"event_type": "put"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
