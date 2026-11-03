"""GD-RS4: Priority resource with a priority for each user.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="R", factor=0, random_seed=42)
    app.add_resource("res", current_cap=1, queue="priority")
    app.add_initial_arrival("u1", size, spawn="user", attributes={"wait": 0, "prio": 0})
    app.add_initial_arrival("u2", 1, spawn="user", attributes={"wait": 1, "prio": 0})
    app.add_initial_arrival("u3", 1, spawn="user", attributes={"wait": 2, "prio": -1})
    user_payload = Template(
        {"event_type": "requesting", "priority": {"gen": "expr", "expr": "attr.prio"}},
        name="user",
    )

    @app.task(name="user", service_time="attr.wait", next="get")
    def user(task_id):
        return user_payload

    @app.task(
        name="get", seize={"resource": "res", "priority": "attr.prio"}, next="hold"
    )
    def get(task_id):
        return {"event_type": "got"}

    @app.task(name="hold", service_time=3, release="res")
    def hold(task_id):
        return {"event_type": "held"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
