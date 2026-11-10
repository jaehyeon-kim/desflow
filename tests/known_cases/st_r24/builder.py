"""ST-R24: Items of equal priority come out in insertion order.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_store("s", current_cap=3, priority=True)
    app.add_initial_arrival(
        "puts", 3, spawn="put_it", attributes={"id": generator({"gen": "sequence"})}
    )
    app.add_arrival("getters", "constant", value=1, first_at=1)

    @app.task(
        name="put_it",
        put={
            "store": "s",
            "priority": 0,
            "item": generator({"gen": "expr", "expr": "attr.id"}),
        },
    )
    def put_it(task_id):
        return {"event_type": "put"}

    get_it_payload = Template(
        {"event_type": "got", "item": {"gen": "expr", "expr": "item"}}, name="get_it"
    )

    @app.task(name="get_it", get={"store": "s"})
    def get_it(task_id):
        return get_it_payload

    @app.arrival_loop("getters")
    def getters_arrivals(context):
        for _ in range(3):
            entity = yield context.wait_for_arrival("getters")
            context.spawn(get_it(context.next_instance("get_it"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
