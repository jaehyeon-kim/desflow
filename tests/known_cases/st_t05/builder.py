"""ST-T05: A child starts after a timeout has been processed.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_signal("early", once=True)
    app.add_initial_arrival("p", size, spawn="parent")

    @app.task(
        name="parent",
        on=[{"fire": {"signal": "early", "after": 1, "value": "i was already done"}}],
        next="later",
    )
    def parent(task_id):
        return {"event_type": "parent"}

    @app.task(name="later", service_time=2, next="child")
    def later(task_id):
        return {"event_type": "later"}

    child_payload = Template(
        {"event_type": "child", "value": {"gen": "expr", "expr": "wait.early.value"}},
        name="child",
    )

    @app.task(name="child", wait={"signal": "early"})
    def child(task_id):
        return child_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
