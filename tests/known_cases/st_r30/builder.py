"""ST-R30: A put into an empty container and two gets.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_container("c", level=0)
    app.add_initial_arrival("p", size, spawn="put_one")
    app.add_initial_arrival("g", 2, spawn="get_one")
    app.add_initial_arrival("look", 1, spawn="look")

    @app.task(name="put_one", put={"container": "c", "amount": 1})
    def put_one(task_id):
        return {"event_type": "put"}

    @app.task(name="get_one", get={"container": "c", "amount": 1})
    def get_one(task_id):
        return {"event_type": "got"}

    look_payload = Template(
        {
            "event_type": "look",
            "level": {"gen": "expr", "expr": "level.c"},
            "getters": {"gen": "expr", "expr": "getters.c"},
            "putters": {"gen": "expr", "expr": "putters.c"},
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
