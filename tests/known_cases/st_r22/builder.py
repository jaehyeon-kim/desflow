"""ST-R22: A get inside a with block is cancelled on leaving.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_store("s", current_cap=1)
    app.add_initial_arrival("a", size, spawn="acquire")

    @app.task(
        name="acquire", open={"g": {"get": {"store": "s"}}}, service_time=1, next="left"
    )
    def acquire(task_id):
        return {"event_type": "waited"}

    left_payload = Template(
        {"event_type": "left_block", "getters": {"gen": "expr", "expr": "getters.s"}},
        name="left",
    )

    @app.task(name="left", cancel="g")
    def left(task_id):
        return left_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
