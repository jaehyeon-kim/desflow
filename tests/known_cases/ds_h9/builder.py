"""DS-H9: A cancel after the grant and a release by handle.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="H9", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_initial_arrival("a", size, spawn="first_job")
    app.add_arrival("b", "constant", value=1)

    @app.task(
        name="first_job",
        open={"slot": {"request": "m"}},
        wait={"handle": "slot"},
        next="cancel_it",
    )
    def first_job(task_id):
        return {"event_type": "opened"}

    @app.task(name="cancel_it", cancel="slot", next="hold_on")
    def cancel_it(task_id):
        return {"event_type": "cancelled_after_grant"}

    @app.task(name="hold_on", service_time=3, release={"handle": "slot"})
    def hold_on(task_id):
        return {"event_type": "still_held"}

    @app.task(name="second_job", resource_id="m", service_time=1)
    def second_job(task_id):
        return {"event_type": "got"}

    @app.arrival_loop("b")
    def b_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("b")
            context.spawn(
                second_job(context.next_instance("second_job"), entity=entity)
            )

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
