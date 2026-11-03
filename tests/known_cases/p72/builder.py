"""P72: A signal that wakes at most a number of waiters.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_store(
        "tok",
    )
    app.add_arrival("w", "constant", value=1, first_at=0)
    app.add_arrival("f", "constant", value=2, first_at=3)

    @app.task(name="waiter", get={"store": "tok"})
    def waiter(task_id):
        return {"event_type": "woke"}

    @app.task(
        name="fire", next={"when": [{"condition": "getters.tok > 0", "task": "give"}]}
    )
    def fire(task_id):
        return {"event_type": "fire"}

    @app.task(name="give", put={"store": "tok"})
    def give(task_id):
        return {"event_type": "give"}

    @app.arrival_loop("w")
    def w_arrivals(context):
        for _ in range(3):
            entity = yield context.wait_for_arrival("w")
            context.spawn(waiter(context.next_instance("waiter"), entity=entity))

    @app.arrival_loop("f")
    def f_arrivals(context):
        for _ in range(2):
            entity = yield context.wait_for_arrival("f")
            context.spawn(fire(context.next_instance("fire"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=10)
