"""CV-030: Schedule an event as urgent.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_signal("x", once=True)
    app.add_initial_arrival("m", size, spawn="make")
    app.add_initial_arrival("w", 1, spawn="wait_x")

    @app.task(name="make", on=[{"fire": {"signal": "x", "urgent": True, "after": 2}}])
    def make(task_id):
        return {"event_type": "made"}

    wait_x_payload = Template(
        {"event_type": "got", "at": {"gen": "expr", "expr": "t"}}, name="wait_x"
    )

    @app.task(name="wait_x", wait={"signal": "x"})
    def wait_x(task_id):
        return wait_x_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
