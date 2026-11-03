"""ST-U11: Chained all-of conditions with intermediate results.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("p", size, spawn="pem")
    pem_payload = Template(
        {
            "event_type": "done",
            "at": {"gen": "expr", "expr": "t"},
            "values": [
                {"gen": "expr", "expr": "wait.a0.value"},
                {"gen": "expr", "expr": "wait.a1.value"},
                {"gen": "expr", "expr": "wait.b0.value"},
                {"gen": "expr", "expr": "wait.b1.value"},
            ],
        },
        name="pem",
    )

    @app.task(
        name="pem",
        wait={
            "all": {
                "a": {
                    "all": {
                        "a0": {"timeout": {"after": 0, "value": 0}},
                        "a1": {"timeout": {"after": 1, "value": 1}},
                    }
                },
                "b": {
                    "all": {
                        "b0": {"timeout": {"after": 0, "value": 0}},
                        "b1": {"timeout": {"after": 1, "value": 1}},
                    }
                },
            }
        },
    )
    def pem(task_id):
        return pem_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
