"""P49: A breakdown on a calendar clock.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 3) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_breakdown(
        "m",
        time_to_failure={"dist": "constant", "value": 3},
        repair={"dist": "constant", "value": 2},
        clock="calendar",
    )
    app.add_initial_arrival("a", size, spawn="job")

    @app.task(name="job", resource_id="m", service_time=1.5, on_interrupt="resume")
    def job(task_id):
        return {"event_type": "done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=40)
