"""CV-127: A capacity or level computed at the start.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1, max_cap=10)
    app.add_variable("n", 4)
    app.add_initial_arrival("setup", size, spawn="setup")

    @app.task(
        name="setup",
        on=[
            {
                "path": "C.resources.m.current_cap",
                "value": generator(
                    {"gen": "expr", "expr": "registry['C.variables.n'] * 2"}
                ),
            }
        ],
    )
    def setup(task_id):
        return {"event_type": "setup"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
