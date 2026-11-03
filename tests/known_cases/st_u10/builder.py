"""ST-U10: Chained all-of conditions.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("p", size, spawn="pem")
    pem_payload = Template(
        {"event_type": "done", "at": {"gen": "expr", "expr": "t"}}, name="pem"
    )

    @app.task(
        name="pem",
        wait={
            "all": {
                "a": {"all": {"a0": {"timeout": 0}, "a1": {"timeout": 1}}},
                "b": {"all": {"b0": {"timeout": 0}, "b1": {"timeout": 1}}},
            }
        },
    )
    def pem(task_id):
        return pem_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
