"""P64: Wake a specific entity chosen by the waker.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 2) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_signal("wake", family=True, once=True)
    app.add_initial_arrival(
        "c",
        size,
        key=lambda n: str(n - 1),
        spawn="sleep",
    )
    app.add_arrival("k", "constant", value=2, attributes={"who": "1"})

    @app.task(name="sleep", wait={"signal": {"of": "wake", "key": "process.key"}})
    def sleep(task_id):
        return {"woken": 1}

    @app.task(
        name="wake_one", on=[{"fire": {"signal": {"of": "wake", "key": "attr.who"}}}]
    )
    def wake_one(task_id):
        return {}

    @app.arrival_loop("k")
    def k_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("k")
            context.spawn(wake_one(context.next_instance("wake_one"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=10)
