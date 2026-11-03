"""P44: One deadline across queues.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 3) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("a", current_cap=1)
    app.add_resource("b", current_cap=1)
    app.add_service("two", dist="constant", value=2)
    app.add_service("four", dist="constant", value=4)
    app.add_initial_arrival("c", size, spawn="first", attributes={"deadline": 5})

    @app.task(
        name="first",
        resource_id="a",
        patience="attr.deadline - t",
        service_id="two",
        next="second",
    )
    def first(task_id):
        return {"event_type": "first_done"}

    @app.task(
        name="second",
        resource_id="b",
        patience="max(0, attr.deadline - t)",
        service_id="four",
    )
    def second(task_id):
        return {"event_type": "done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
