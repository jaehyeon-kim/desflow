"""ST-P05: A child returns an exception object as a value.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("p", size, spawn="parent")
    parent_payload = Template(
        {
            "event_type": "got",
            "value": {"gen": "expr", "expr": "wait.child.value.error"},
        },
        name="parent",
    )

    @app.task(name="parent", wait={"task": "child"})
    def parent(task_id):
        return parent_payload

    @app.task(name="child", service_time=1)
    def child(task_id):
        return {"error": "Onoes!"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
