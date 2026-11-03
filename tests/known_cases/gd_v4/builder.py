"""GD-V4: A process is an event and its value is returned by the run.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="V", factor=0, random_seed=42)
    app.add_signal("done", once=True)
    app.add_initial_arrival("a", size, spawn="parent")
    parent_payload = Template(
        {"ret": {"gen": "expr", "expr": "wait.sub.value.ret"}}, name="parent"
    )

    @app.task(name="parent", wait={"task": "sub"}, signal="done")
    def parent(task_id):
        return parent_payload

    @app.task(name="sub", service_time=1)
    def sub(task_id):
        return {"ret": 23}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(stop={"signal": "done"})
