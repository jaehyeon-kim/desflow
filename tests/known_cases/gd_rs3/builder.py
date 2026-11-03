"""GD-RS3: Read count, capacity, users and queue around a request.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 2) -> SimulationContext:
    app = SimulationContext(sim_id="R", factor=0, random_seed=42)
    app.add_resource("res", current_cap=1)
    app.add_initial_arrival("a", size, spawn="user")
    user_payload = Template(
        {
            "event_type": "before",
            "count": {"gen": "expr", "expr": "users.res"},
            "queued": {"gen": "expr", "expr": "queue.res"},
            "capacity": {
                "gen": "expr",
                "expr": "registry['R.resources.res.current_cap']",
            },
        },
        name="user",
    )

    @app.task(name="user", next="use")
    def user(task_id):
        return user_payload

    use_payload = Template(
        {
            "event_type": "during",
            "count": {"gen": "expr", "expr": "users.res"},
            "queued": {"gen": "expr", "expr": "queue.res"},
            "capacity": {
                "gen": "expr",
                "expr": "registry['R.resources.res.current_cap']",
            },
        },
        name="use",
    )

    @app.task(name="use", seize="res", release="res", next="after")
    def use(task_id):
        return use_payload

    after_payload = Template(
        {
            "event_type": "after",
            "count": {"gen": "expr", "expr": "users.res"},
            "queued": {"gen": "expr", "expr": "queue.res"},
            "capacity": {
                "gen": "expr",
                "expr": "registry['R.resources.res.current_cap']",
            },
        },
        name="after",
    )

    @app.task(name="after")
    def after(task_id):
        return after_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
