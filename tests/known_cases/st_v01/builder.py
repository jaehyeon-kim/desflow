"""ST-V01: A parent succeeds an event that a child waits for.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_signal("ev", once=True)
    app.add_initial_arrival("p", size, spawn="parent")
    child_payload = Template(
        {"event_type": "got", "value": {"gen": "expr", "expr": "wait.ev.value"}},
        name="child",
    )

    @app.task(name="child", wait={"signal": "ev"})
    def child(task_id):
        return child_payload

    @app.task(
        name="parent",
        spawn={"task": "child"},
        service_time=5,
        on=[{"fire": {"signal": "ev", "value": "ohai"}}],
    )
    def parent(task_id):
        return {"event_type": "parent"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
