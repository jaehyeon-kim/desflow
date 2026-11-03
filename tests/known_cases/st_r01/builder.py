"""ST-R01: Two users share one unit with explicit request and release.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 2) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_resource("res", current_cap=1)
    app.add_initial_arrival("users", size, spawn="pem")
    pem_payload = Template(
        {"event_type": "got", "count": {"gen": "expr", "expr": "users.res"}}, name="pem"
    )

    @app.task(name="pem", seize="res", next="use")
    def pem(task_id):
        return pem_payload

    @app.task(name="use", service_time=1, release="res")
    def use(task_id):
        return {"event_type": "done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
