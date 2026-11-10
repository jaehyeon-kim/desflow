"""DX-10: A job that keeps its unit through two steps.

Builder form of `model.yaml`.
"""

from desflow import NO_RECORD, SimulationContext
from desflow.values import Template


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_resource("machine", current_cap=1, queue="priority")
    for name, first, job, priority, one, two in (
        ("j1", 0, "job-1", 2, 1, 2),
        ("j2", 0, "job-2", 1, 2, 1),
        ("j3", 1, "job-3", 0, 1, 1),
    ):
        app.add_arrival(
            name,
            "constant",
            value=1,
            spawn="repair",
            max_events=1,
            first_at=first,
            attributes={"job": job, "priority": priority, "first": one, "second": two},
        )

    @app.task(
        name="repair",
        seize={"resource": "machine", "priority": "attr.priority"},
        service_time="attr.first",
        next="finish",
    )
    def repair(task_id):
        return NO_RECORD

    @app.task(name="finish", service_time="attr.second", release="machine")
    def finish(task_id):
        return Template({"job": {"gen": "attr", "name": "job"}}, name="tasks.finish")

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=20)
