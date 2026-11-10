"""ST-U19: An all-of expansion is one flat condition.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="pem")
    pem_payload = Template(
        {
            "event_type": "done",
            "count": {
                "gen": "expr",
                "expr": "int(wait.a.done) + int(wait.b.done) + int(wait.c.done)",
            },
        },
        name="pem",
    )

    @app.task(
        name="pem",
        wait={"all": {"a": {"timeout": 3}, "b": {"timeout": 2}, "c": {"timeout": 1}}},
    )
    def pem(task_id):
        return pem_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
