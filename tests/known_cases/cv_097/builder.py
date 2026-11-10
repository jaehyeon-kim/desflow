"""CV-097: Read the value of a get.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_store("s", items=[{"v": 7}])
    app.add_initial_arrival("a", size, spawn="take")
    take_payload = Template(
        {"event_type": "got", "v": {"gen": "expr", "expr": "wait.g.value.v"}},
        name="take",
    )

    @app.task(name="take", wait={"all": {"g": {"get": {"store": "s"}}}})
    def take(task_id):
        return take_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
