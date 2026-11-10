"""ST-R20: A store with no capacity is unbounded.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_store(
        "plain",
    )
    app.add_store(
        "filtered",
    )
    app.add_initial_arrival("a", size, spawn="read")
    read_payload = Template(
        {
            "event_type": "capacity",
            "plain": {"gen": "expr", "expr": "registry['T.stores.plain.current_cap']"},
            "filtered": {
                "gen": "expr",
                "expr": "registry['T.stores.filtered.current_cap']",
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
