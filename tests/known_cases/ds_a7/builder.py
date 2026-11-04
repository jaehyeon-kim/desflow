"""DS-A7: A spawn count and copied attributes.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build() -> SimulationContext:
    app = SimulationContext(sim_id="A7", factor=0, random_seed=42)
    app.add_arrival(
        "a", "constant", value=3, first_at=0, attributes={"n": 3, "tag": "x"}
    )

    @app.task(
        name="parent",
        spawn={"task": "clone", "count": "attr.n", "copy_attributes": True},
    )
    def parent(task_id):
        return {"event_type": "parent"}

    clone_payload = Template(
        {"event_type": "clone", "tag": {"gen": "attr", "name": "tag"}}, name="clone"
    )

    @app.task(name="clone")
    def clone(task_id):
        return clone_payload

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(2):
            entity = yield context.wait_for_arrival("a")
            context.spawn(parent(context.next_instance("parent"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
