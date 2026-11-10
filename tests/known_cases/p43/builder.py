"""P43: Renege when a signal fires.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 3) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_initial_arrival("a", size, spawn="job")
    app.add_arrival("bell", "constant", value=2)

    @app.task(
        name="job",
        resource_id="m",
        service_time=2.5,
        on_interrupt="abandon",
        on_interrupt_next="other",
    )
    def job(task_id):
        return {"event_type": "served"}

    @app.task(name="other")
    def other(task_id):
        return {"event_type": "other"}

    @app.task(
        name="ring", interrupt={"task": "job", "where": "process.target != 'service'"}
    )
    def ring(task_id):
        return {"event_type": "bell"}

    @app.arrival_loop("bell")
    def bell_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("bell")
            context.spawn(ring(context.next_instance("ring"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
