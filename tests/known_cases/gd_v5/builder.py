"""GD-V5: A process started after a delay, then waited for.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="V", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="parent")
    parent_payload = Template(
        {
            "event_type": "parent_done",
            "ret": {"gen": "expr", "expr": "wait.delayed_sub.value.ret"},
        },
        name="parent",
    )

    @app.task(name="parent", wait={"task": "delayed_sub"})
    def parent(task_id):
        return parent_payload

    @app.task(name="delayed_sub", service_time=3, next="sub")
    def delayed_sub(task_id):
        return {"event_type": "starting"}

    @app.task(name="sub", service_time=1)
    def sub(task_id):
        return {"ret": 23}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
