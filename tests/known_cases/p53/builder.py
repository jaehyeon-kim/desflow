"""P53: Batch and separate with a rule.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_initial_arrival("m0", size, spawn="pack")
    app.add_arrival("m1", "constant", value=1)
    app.add_arrival("m2", "constant", value=5)
    app.add_arrival("m3", "constant", value=5.5)
    app.add_arrival("m4", "constant", value=5.8)

    @app.task(
        name="pack",
        batch={"name": "box", "size": 3, "timeout": 2, "close_when": "batch.size >= 2"},
    )
    def pack(task_id):
        return {"event_type": "packed"}

    @app.arrival_loop("m1")
    def m1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("m1")
            context.spawn(pack(context.next_instance("pack"), entity=entity))

    @app.arrival_loop("m2")
    def m2_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("m2")
            context.spawn(pack(context.next_instance("pack"), entity=entity))

    @app.arrival_loop("m3")
    def m3_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("m3")
            context.spawn(pack(context.next_instance("pack"), entity=entity))

    @app.arrival_loop("m4")
    def m4_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("m4")
            context.spawn(pack(context.next_instance("pack"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=30)
