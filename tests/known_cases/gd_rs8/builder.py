"""GD-RS8: A producer and two consumers on a store of two.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="R", factor=0, random_seed=42)
    app.add_store("buf", current_cap=2)
    app.add_initial_arrival("producer", size, spawn="produce", attributes={"i": 0})
    app.add_initial_arrival("consumers", 2, spawn="consume")

    @app.task(
        name="produce",
        service_time=2,
        put={
            "store": "buf",
            "item": generator(
                {
                    "gen": "format",
                    "template": "spam {i}",
                    "names": {"i": {"gen": "attr", "name": "i"}},
                }
            ),
        },
        set={"i": generator({"gen": "expr", "expr": "attr.i + 1"})},
        next={"when": [{"condition": "attr.i < 100", "task": "produce"}]},
    )
    def produce(task_id):
        return {"event_type": "produced"}

    @app.task(name="consume", service_time=1, next="take")
    def consume(task_id):
        return {"event_type": "requesting"}

    take_payload = Template(
        {"event_type": "got", "item": {"gen": "expr", "expr": "item"}}, name="take"
    )

    @app.task(name="take", get={"store": "buf"}, next="consume")
    def take(task_id):
        return take_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=5)
