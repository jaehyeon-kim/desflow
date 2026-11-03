"""P26: A sub-process that returns a value.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="parent")
    parent_payload = Template(
        {"event_type": "got", "v": {"gen": "expr", "expr": "wait.child.value.v"}},
        name="parent",
    )

    @app.task(name="parent", wait={"task": "child"})
    def parent(task_id):
        return parent_payload

    @app.task(name="child", service_time=2)
    def child(task_id):
        return {"v": 20}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
