"""ST-E01: The clock ends at the run time when the queue is empty.

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

    @app.task(name="pem", next="pause")
    def pem(task_id):
        return pem_payload

    @app.task(
        name="pause",
        service_time=1,
        next={"when": [{"condition": "t < 2", "task": "pem"}]},
    )
    def pause(task_id):
        return {"event_type": "tick"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=10)
