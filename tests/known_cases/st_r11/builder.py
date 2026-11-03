"""ST-R11: Read the users and queue of a resource.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 3) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_resource("res", current_cap=1)
    app.add_initial_arrival("users", size, spawn="pem")

    @app.task(name="pem", resource_id="res", service_time=1)
    def pem(task_id):
        return {"event_type": "used"}

    look_payload = Template(
        {
            "event_type": "look",
            "users": {"gen": "expr", "expr": "[h.entity for h in held(res)]"},
            "queue": {"gen": "expr", "expr": "[w.entity for w in waiting(res)]"},
        },
        name="look",
    )

    @app.task(name="look")
    def look(task_id):
        return look_payload

    return app


def run(app: SimulationContext):
    """Runs the model with the `run` section of `model.yaml`."""
    return app.run(
        until=2,
        segments=[{"until": 1, "look": "look"}, {"until": 2, "look": "look"}],
    )
