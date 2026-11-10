"""ST-C09: A failing event in a condition that nobody handles stops the run.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="pem")
    pem_payload = Template(
        {"event_type": "done", "at": {"gen": "expr", "expr": "t"}}, name="pem"
    )

    @app.task(name="pem", wait={"any": {"t": {"timeout": 1}, "x": {"task": "explode"}}})
    def pem(task_id):
        return pem_payload

    @app.task(
        name="explode",
        service_time=2,
        fail="True",
        fail_reason="Onoes, failed after 2!",
    )
    def explode(task_id):
        return {"event_type": "child"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(on_unhandled_failure="stop")
