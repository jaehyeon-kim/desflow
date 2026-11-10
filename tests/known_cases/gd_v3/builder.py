"""GD-V3: A shared event made again after each firing.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 3) -> SimulationContext:
    app = SimulationContext(sim_id="V", factor=0, random_seed=42)
    app.add_signal("class_ends", once=False)
    app.add_initial_arrival("pupils", size, spawn="first_class")
    app.add_arrival("bell", "constant", value=45)

    @app.task(name="first_class", wait={"signal": "class_ends"}, next="second_class")
    def first_class(task_id):
        return {"event_type": "class_over"}

    @app.task(name="second_class", wait={"signal": "class_ends"})
    def second_class(task_id):
        return {"event_type": "class_over_again"}

    @app.task(name="ring", signal="class_ends")
    def ring(task_id):
        return {"event_type": "bell"}

    @app.arrival_loop("bell")
    def bell_arrivals(context):
        for _ in range(2):
            entity = yield context.wait_for_arrival("bell")
            context.spawn(ring(context.next_instance("ring"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
