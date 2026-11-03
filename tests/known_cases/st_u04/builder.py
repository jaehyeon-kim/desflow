"""ST-U04: A subscriber interrupted while waiting for another child.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("p", size, spawn="parent")

    @app.task(
        name="parent",
        subscribe={"task": "child1"},
        wait={"task": "child2"},
        on_interrupt="abandon",
        on_interrupt_next="told",
    )
    def parent(task_id):
        return {"event_type": "joined"}

    told_payload = Template(
        {"event_type": "interrupted", "at": {"gen": "expr", "expr": "t"}}, name="told"
    )

    @app.task(name="told")
    def told(task_id):
        return told_payload

    @app.task(name="child1", service_time=1)
    def child1(task_id):
        return {"event_type": "c1"}

    @app.task(name="child2", service_time=2)
    def child2(task_id):
        return {"event_type": "c2"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
