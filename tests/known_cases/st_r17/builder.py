"""ST-R17: Read the queues of a container of capacity 1.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_container("c", current_cap=1, level=0)
    app.add_initial_arrival("g", size, spawn="get_it")
    app.add_initial_arrival("p", 3, spawn="put_it")

    @app.task(name="get_it", get={"container": "c", "amount": 1})
    def get_it(task_id):
        return {"event_type": "got"}

    @app.task(name="put_it", wait={"timeout": 1}, put={"container": "c", "amount": 1})
    def put_it(task_id):
        return {"event_type": "put"}

    look_payload = Template(
        {
            "event_type": "look",
            "put_queue": {"gen": "expr", "expr": "[p.entity for p in putting(c)]"},
            "get_queue": {"gen": "expr", "expr": "[g.entity for g in getting(c)]"},
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
