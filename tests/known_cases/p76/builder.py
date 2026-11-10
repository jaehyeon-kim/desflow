"""P76: Enter a queue sorted or at the head.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_store("q", get_queue={"rank": "attr.rank", "ties": "lifo"})
    app.add_arrival(
        "g",
        "constant",
        value=0.5,
        attributes={
            "rank": generator(
                {"gen": "sequence", "start": 3, "step": -1, "loop_after": 4}
            )
        },
    )
    app.add_arrival("p", "constant", value=2, first_at=1)

    @app.task(name="want", get={"store": "q"})
    def want(task_id):
        return {"event_type": "got"}

    @app.task(name="supply", put={"store": "q"})
    def supply(task_id):
        return {"event_type": "supplied"}

    @app.arrival_loop("g")
    def g_arrivals(context):
        for _ in range(4):
            entity = yield context.wait_for_arrival("g")
            context.spawn(want(context.next_instance("want"), entity=entity))

    @app.arrival_loop("p")
    def p_arrivals(context):
        for _ in range(4):
            entity = yield context.wait_for_arrival("p")
            context.spawn(supply(context.next_instance("supply"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
