"""GD-TT5: An event and an interrupt arrive together.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_signal("coup", once=True)
    app.add_initial_arrival("fox", size, spawn="fox")
    app.add_arrival("plan", "constant", value=1)

    @app.task(name="fox", wait={"signal": "coup"}, on_interrupt="resume")
    def fox(task_id):
        return {"event_type": "coup_completed"}

    @app.task(name="master_plan", signal="coup", interrupt={"task": "fox"})
    def master_plan(task_id):
        return {"event_type": "plan"}

    @app.arrival_loop("plan")
    def plan_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("plan")
            context.spawn(
                master_plan(context.next_instance("master_plan"), entity=entity)
            )

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=5)
