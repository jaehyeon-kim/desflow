"""DX-6: A process that takes every packer off for a break.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator

CAPACITY = "Shop.resources.packers.current_cap"


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=1)
    app.add_resource("packers", current_cap=3, max_cap=3)
    app.add_variable("normal", 3)
    app.add_initial_arrival("clock", size, spawn="down")

    @app.task(
        name="down",
        wait={"timeout": 50},
        on=[{"path": CAPACITY, "value": 0}],
        key="packers-break",
        next="up",
    )
    def down(task_id):
        return {"status": "down"}

    @app.task(
        name="up",
        wait={"timeout": 5},
        on=[
            {
                "path": CAPACITY,
                "value": generator(
                    {"gen": "expr", "expr": "registry['Shop.variables.normal']"}
                ),
            }
        ],
        key="packers-break",
        next="down",
    )
    def up(task_id):
        return {"status": "up"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=180)
