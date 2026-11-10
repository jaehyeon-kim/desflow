"""GD-B1: A timeout that carries a value.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="B", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="example")
    example_payload = Template(
        {
            "event_type": "printed",
            "value": {"gen": "expr", "expr": "wait.timeout.value"},
        },
        name="example",
    )

    @app.task(name="example", wait={"timeout": {"after": 1, "value": 42}})
    def example(task_id):
        return example_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
