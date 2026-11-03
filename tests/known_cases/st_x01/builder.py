"""ST-X01: A failing child that the parent catches.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("p", size, spawn="parent")

    @app.task(name="parent", wait={"task": "child"}, on_fail="caught")
    def parent(task_id):
        return {"event_type": "no_error"}

    caught_payload = Template(
        {"event_type": "caught", "reason": {"gen": "expr", "expr": "upstream.reason"}},
        name="caught",
    )

    @app.task(name="caught")
    def caught(task_id):
        return caught_payload

    @app.task(name="child", fail="True", fail_reason="Onoes!")
    def child(task_id):
        return {"event_type": "child"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
