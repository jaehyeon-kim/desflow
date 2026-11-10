"""GD-TT1: Interrupted while joining a child, then join it again.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("p", 1, spawn="parent")
    app.add_arrival("i", "constant", value=1)

    @app.task(name="parent", wait={"task": "child"}, on_interrupt="resume")
    def parent(task_id):
        return {"event_type": "joined"}

    @app.task(name="child", service_time=2)
    def child(task_id):
        return {"event_type": "child_done"}

    @app.task(name="interruptor", interrupt={"task": "parent"})
    def interruptor(task_id):
        return {"event_type": "interrupt"}

    @app.arrival_loop("i")
    def i_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("i")
            context.spawn(
                interruptor(context.next_instance("interruptor"), entity=entity)
            )

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
