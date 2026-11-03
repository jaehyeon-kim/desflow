"""GD-V8: Read the values of an all-of condition in order.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="V", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="both")
    both_payload = Template(
        {
            "values": [
                {"gen": "expr", "expr": "wait.t1.value"},
                {"gen": "expr", "expr": "wait.t2.value"},
            ]
        },
        name="both",
    )

    @app.task(
        name="both",
        wait={
            "all": {
                "t1": {"timeout": {"after": 1, "value": "spam"}},
                "t2": {"timeout": {"after": 2, "value": "eggs"}},
            }
        },
    )
    def both(task_id):
        return both_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
