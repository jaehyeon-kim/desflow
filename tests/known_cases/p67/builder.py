"""P67: Stop when a condition holds.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_container("stock", current_cap=20, max_cap=20)
    app.add_arrival("restocks", "constant", value=3, spawn="restock")
    app.add_arrival("pings", "constant", value=4, spawn="ping")

    @app.task(name="restock", put={"container": "stock", "amount": 2})
    def restock(task_id):
        return {"event": "restocked"}

    @app.task(name="ping")
    def ping(task_id):
        return {"event": "ping"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=100, stop={"condition": "level.stock >= 5"})
