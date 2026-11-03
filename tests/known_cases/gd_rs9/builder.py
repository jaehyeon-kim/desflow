"""GD-RS9: A filter store of machines.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="R", factor=0, random_seed=42)
    app.add_store(
        "machines",
        current_cap=2,
        items=[{"size": 1, "duration": 2}, {"size": 2, "duration": 1}],
    )
    app.add_initial_arrival("u0", size, spawn="take", attributes={"size": 1})
    app.add_initial_arrival("u1", 1, spawn="take", attributes={"size": 2})
    app.add_initial_arrival("u2", 1, spawn="take", attributes={"size": 1})
    take_payload = Template(
        {
            "event_type": "took",
            "size": {"gen": "expr", "expr": "item.size"},
            "duration": {"gen": "expr", "expr": "item.duration"},
        },
        name="take",
    )

    @app.task(
        name="take",
        get={"store": "machines", "filter": "item.size == attr.size"},
        next="use",
    )
    def take(task_id):
        return take_payload

    @app.task(name="use", service_time="item.duration", next="back")
    def use(task_id):
        return {"event_type": "used"}

    @app.task(
        name="back",
        put={"store": "machines", "item": generator({"gen": "expr", "expr": "item"})},
    )
    def back(task_id):
        return {"event_type": "released"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
