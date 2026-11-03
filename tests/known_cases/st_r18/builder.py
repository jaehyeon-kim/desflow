"""ST-R18: A container with no capacity is unbounded.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_container("c", level=0)
    app.add_initial_arrival("a", size, spawn="read")
    read_payload = Template(
        {
            "event_type": "capacity",
            "capacity": {
                "gen": "expr",
                "expr": "registry['T.containers.c.current_cap']",
            },
        },
        name="read",
    )

    @app.task(name="read")
    def read(task_id):
        return read_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
