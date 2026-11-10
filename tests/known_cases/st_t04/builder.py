"""ST-T04: Three processes wait for the same timeout.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 3) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("kids", size, spawn="child")
    child_payload = Template(
        {"event_type": "woke", "at": {"gen": "expr", "expr": "t"}}, name="child"
    )

    @app.task(name="child", wait={"timeout": 1})
    def child(task_id):
        return child_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
