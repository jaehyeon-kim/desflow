"""ST-R19: A putter and a getter on a store of two.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_store("s", current_cap=2)
    app.add_initial_arrival("p", size, spawn="putter")
    app.add_initial_arrival("g", 1, spawn="getter")

    @app.task(name="putter", put={"store": "s", "item": {"id": 1}})
    def putter(task_id):
        return {"event_type": "put"}

    getter_payload = Template(
        {"event_type": "got", "same": {"gen": "expr", "expr": "item.id == 1"}},
        name="getter",
    )

    @app.task(name="getter", get={"store": "s"})
    def getter(task_id):
        return getter_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
