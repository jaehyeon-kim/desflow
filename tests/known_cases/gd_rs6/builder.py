"""GD-RS6: A preempt flag for each request.

Builder form of `model.yaml`.
"""

from pathlib import Path

from desflow import SimulationContext

HERE = Path(__file__).parent


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="R", factor=0, random_seed=42)
    app.add_resource("res", current_cap=1, queue="priority", preempt=True)
    app.add_initial_arrival(
        "a", size, spawn="user", attributes={"prio": 0, "can_preempt": True}
    )
    app.add_arrival_replay(
        "late",
        HERE / "late.csv",
        time="time",
        unit="s",
        fields=["prio", "can_preempt"],
        spawn="user",
    )

    @app.task(name="user", next="get")
    def user(task_id):
        return {"event_type": "requesting"}

    @app.task(
        name="get",
        seize={
            "resource": "res",
            "priority": "attr.prio",
            "preempt": "attr.can_preempt == 1",
        },
        next="hold",
    )
    def get(task_id):
        return {"event_type": "got"}

    @app.task(
        name="hold",
        service_time=3,
        release="res",
        on_interrupt="abandon",
        on_interrupt_next="preempted",
    )
    def hold(task_id):
        return {"event_type": "held"}

    @app.task(name="preempted")
    def preempted(task_id):
        return {"event_type": "preempted"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
