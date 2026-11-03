"""ST-U15: Chained any-of conditions.

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
            "at": {"gen": "expr", "expr": "t"},
            "values": [{"gen": "expr", "expr": "wait.b.value"}],
        },
        name="pem",
    )

    @app.task(
        name="pem",
        wait={
            "any": {
                "a": {"timeout": {"after": 2, "value": "a"}},
                "b": {"timeout": {"after": 1, "value": "b"}},
            }
        },
    )
    def pem(task_id):
        return pem_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
