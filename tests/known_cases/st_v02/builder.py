"""ST-V02: A failed event that a child catches.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_signal("ev", once=True)
    app.add_initial_arrival("p", size, spawn="parent")

    @app.task(name="child", wait={"signal": "ev"}, on_fail="caught")
    def child(task_id):
        return {"event_type": "fine"}

    caught_payload = Template(
        {"event_type": "caught", "reason": {"gen": "expr", "expr": "upstream.reason"}},
        name="caught",
    )

    @app.task(name="caught")
    def caught(task_id):
        return caught_payload

    @app.task(
        name="parent",
        spawn={"task": "child"},
        service_time=5,
        on=[{"fire": {"signal": "ev", "fail": True, "value": "ohai"}}],
    )
    def parent(task_id):
        return {"event_type": "parent"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
