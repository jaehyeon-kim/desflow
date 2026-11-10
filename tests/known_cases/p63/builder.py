"""P63: Broadcast a signal.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 2) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_signal(
        "wake",
    )
    app.add_initial_arrival("c", size, spawn="waitc")
    app.add_arrival("k", "constant", value=2)

    @app.task(name="waitc", wait={"signal": "wake"})
    def waitc(task_id):
        return {"woken": 1}

    @app.task(name="clerk", signal="wake")
    def clerk(task_id):
        return {}

    @app.arrival_loop("k")
    def k_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("k")
            context.spawn(clerk(context.next_instance("clerk"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=10)
