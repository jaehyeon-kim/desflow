"""CV-131: A callback that interrupts or spawns.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_signal(
        "bell",
        once=True,
        on=[
            {"interrupt": {"task": "sleeper", "cause": "bell"}},
            {"spawn": {"task": "helper"}},
        ],
    )
    app.add_initial_arrival("s", size, spawn="sleeper")
    app.add_arrival("r", "constant", value=2)

    @app.task(name="sleeper", service_time=10, on_interrupt="abandon")
    def sleeper(task_id):
        return {"event_type": "slept"}

    @app.task(name="helper")
    def helper(task_id):
        return {"event_type": "helper"}

    @app.task(name="ring", signal="bell")
    def ring(task_id):
        return {"event_type": "ring"}

    @app.arrival_loop("r")
    def r_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("r")
            context.spawn(ring(context.next_instance("ring"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
