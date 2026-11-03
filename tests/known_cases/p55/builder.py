"""P55: Match by key with a filter.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_store("docs", current_cap=10)
    app.add_arrival("d7", "constant", value=1, attributes={"order": 7})
    app.add_arrival("d8", "constant", value=2, attributes={"order": 8})
    app.add_initial_arrival("box", size, spawn="match", attributes={"order": 8})

    @app.task(name="file", put={"store": "docs"})
    def file(task_id, entity):
        return {"order": entity.attributes.get("order")}

    @app.task(name="match", get={"store": "docs", "filter": "item.order == attr.order"})
    def match(task_id):
        return {"matched": 1}

    @app.arrival_loop("d7")
    def d7_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("d7")
            context.spawn(file(context.next_instance("file"), entity=entity))

    @app.arrival_loop("d8")
    def d8_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("d8")
            context.spawn(file(context.next_instance("file"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=10)
