"""ST-U01: A child started after a delay.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("p", size, spawn="parent")

    @app.task(name="parent", spawn={"task": "child", "after": 5})
    def parent(task_id):
        return {"event_type": "parent"}

    child_payload = Template(
        {"event_type": "started", "at": {"gen": "expr", "expr": "t"}}, name="child"
    )

    @app.task(name="child")
    def child(task_id):
        return child_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
