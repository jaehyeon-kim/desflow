"""P16: Set a constant attribute.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="t")

    @app.task(name="t", set={"stage": 2}, next="u")
    def t(task_id):
        return {"event_type": "set"}

    u_payload = Template(
        {"event_type": "read", "stage": {"gen": "attr", "name": "stage"}}, name="u"
    )

    @app.task(name="u")
    def u(task_id):
        return u_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
