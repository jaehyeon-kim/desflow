"""ST-P01: Read whether processes are alive.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="pem_a")
    app.add_initial_arrival("b", 1, spawn="pem_b")

    @app.task(name="pem_a", service_time=3)
    def pem_a(task_id):
        return {"event_type": "a_done"}

    pem_b_payload = Template(
        {
            "event_type": "b_first",
            "a_alive": {"gen": "expr", "expr": "running(pem_a) > 0"},
        },
        name="pem_b",
    )

    @app.task(name="pem_b", service_time=1, next="pem_b2")
    def pem_b(task_id):
        return pem_b_payload

    pem_b2_payload = Template(
        {
            "event_type": "b_done",
            "a_alive": {"gen": "expr", "expr": "running(pem_a) > 0"},
        },
        name="pem_b2",
    )

    @app.task(name="pem_b2", service_time=3)
    def pem_b2(task_id):
        return pem_b2_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
