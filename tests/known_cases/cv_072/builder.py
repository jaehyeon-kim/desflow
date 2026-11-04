"""CV-072: Wait on several requests.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_resource("n", current_cap=1)
    app.add_initial_arrival("blockers", size, spawn="block")
    app.add_arrival("a", "constant", value=1)

    @app.task(name="block", resource_id="m", service_time=5)
    def block(task_id):
        return {"event_type": "blocked"}

    either_payload = Template(
        {"event_type": "served", "winner": {"gen": "expr", "expr": "wait.b.done"}},
        name="either",
    )

    @app.task(
        name="either",
        wait={"any": {"a": {"request": "m"}, "b": {"request": "n"}}},
        service_time=1,
    )
    def either(task_id):
        return either_payload

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(2):
            entity = yield context.wait_for_arrival("a")
            context.spawn(either(context.next_instance("either"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
