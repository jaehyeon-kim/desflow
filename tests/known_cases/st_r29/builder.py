"""ST-R29: The first request of an empty resource is granted at once.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 2) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_resource("res", current_cap=1)
    app.add_initial_arrival("a", size, spawn="ask")
    app.add_initial_arrival("look", 1, spawn="look")

    @app.task(name="ask", open={"slot": {"request": "res"}})
    def ask(task_id):
        return {"event_type": "asked"}

    look_payload = Template(
        {
            "event_type": "look",
            "users": {"gen": "expr", "expr": "users.res"},
            "queued": {"gen": "expr", "expr": "queue.res"},
        },
        name="look",
    )

    @app.task(name="look")
    def look(task_id):
        return look_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
