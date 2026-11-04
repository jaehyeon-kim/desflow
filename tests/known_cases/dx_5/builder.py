"""DX-5: A payload that reads which branches of a wait were done.

Builder form of `model.yaml`.
"""

from pathlib import Path

from desflow import SimulationContext

HERE = Path(__file__).parent


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_signal("restocked", once=False)
    app.add_arrival_replay("orders", HERE / "orders.csv", spawn="order")
    app.add_arrival_replay("restocks", HERE / "restocks.csv", spawn="restock")

    @app.task(
        name="order",
        wait={"any": {"stock": {"signal": "restocked"}, "limit": {"timeout": 30}}},
    )
    def order(task_id, wait):
        return {
            "event_type": "resumed",
            "done": {name: branch["done"] for name, branch in wait.items()},
        }

    @app.task(name="restock", signal="restocked")
    def restock(task_id):
        return {"boxes": 6}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=60)
