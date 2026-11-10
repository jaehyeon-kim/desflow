"""ST-U07: All of a list of ten timeouts.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival(
        "a", size, spawn="pem", attributes={"delays": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]}
    )
    pem_payload = Template(
        {"event_type": "all_done", "at": {"gen": "expr", "expr": "t"}}, name="pem"
    )

    @app.task(
        name="pem",
        wait={
            "each": "attr.delays",
            "group": "all",
            "branch": {"timeout": "each.value"},
        },
    )
    def pem(task_id):
        return pem_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
