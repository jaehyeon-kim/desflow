"""P23: A probability from an attribute or the state.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_arrival("a", "constant", value=1, attributes={"w": 0.8})

    @app.task(name="pick", next={"weights": {"fast": "attr.w", "slow": "1 - attr.w"}})
    def pick(task_id):
        return {"event_type": "pick"}

    @app.task(name="fast")
    def fast(task_id):
        return {"event_type": "fast"}

    @app.task(name="slow")
    def slow(task_id):
        return {"event_type": "slow"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(6):
            entity = yield context.wait_for_arrival("a")
            context.spawn(pick(context.next_instance("pick"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
