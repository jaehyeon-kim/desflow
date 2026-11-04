"""DS-M6: The flow name of a process.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="M6", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="worker")
    worker_payload = Template(
        {"event_type": "one", "name": {"gen": "expr", "expr": "process.flow_name"}},
        name="worker",
    )

    @app.task(name="worker", next="two")
    def worker(task_id):
        return worker_payload

    two_payload = Template(
        {"event_type": "two", "name": {"gen": "expr", "expr": "process.flow_name"}},
        name="two",
    )

    @app.task(name="two", service_time=1, next="three")
    def two(task_id):
        return two_payload

    three_payload = Template(
        {"event_type": "three", "name": {"gen": "expr", "expr": "process.flow_name"}},
        name="three",
    )

    @app.task(name="three", service_time=1)
    def three(task_id):
        return three_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
