"""ST-T01: A loop that logs the time every second.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="pem")
    pem_payload = Template(
        {"event_type": "now", "now": {"gen": "expr", "expr": "t"}}, name="pem"
    )

    @app.task(name="pem", next="step")
    def pem(task_id):
        return pem_payload

    @app.task(name="step", service_time=1, next="pem")
    def step(task_id):
        return {"event_type": "step"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=3)
