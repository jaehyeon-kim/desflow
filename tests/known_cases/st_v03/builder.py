"""ST-V03: A timeout keeps its value after the run.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="pem")
    pem_payload = Template(
        {"event_type": "got", "value": {"gen": "expr", "expr": "wait.timeout.value"}},
        name="pem",
    )

    @app.task(name="pem", wait={"timeout": {"after": 0, "value": "I am the value"}})
    def pem(task_id):
        return pem_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
