"""P50: A failure clock that runs on busy time only.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("mach", current_cap=1, queue="priority", preempt=True)
    app.add_resource("rep", current_cap=1, queue="priority")
    app.add_service("w", dist="constant", value=10)
    app.add_service("o", dist="constant", value=6)
    app.add_service("ttf", dist="constant", value=5)
    app.add_service("fix", dist="constant", value=2)
    app.add_initial_arrival("jobs", size, spawn="job")
    app.add_initial_arrival("others", 1, spawn="other")
    app.add_initial_arrival("clock", 1, spawn="tick")

    @app.task(
        name="job",
        service_id="w",
        resource_id="mach",
        priority=1,
        on_interrupt="resume",
    )
    def job(task_id):
        return {"event_type": "job_done"}

    @app.task(name="other", service_id="o", resource_id="rep")
    def other(task_id):
        return {"event_type": "other_done"}

    @app.task(name="tick", service_id="ttf", next="repair")
    def tick(task_id):
        return {"event_type": "tick"}

    @app.task(
        name="repair",
        service_id="fix",
        resource_id=["mach", "rep"],
        priority=-1,
        next="tick",
    )
    def repair(task_id):
        return {"event_type": "repaired"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=16)
