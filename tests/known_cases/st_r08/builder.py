"""ST-R08: A request or a timeout, whichever comes first.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_resource("res", current_cap=1)
    app.add_initial_arrival("a", size, spawn="pem")
    pem_payload = Template(
        {"event_type": "done", "got_it": {"gen": "expr", "expr": "wait.req.done"}},
        name="pem",
    )

    @app.task(
        name="pem", wait={"any": {"req": {"request": "res"}, "t": {"timeout": 1}}}
    )
    def pem(task_id):
        return pem_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
