"""P79: Change the priority rules with preempt and restart flags.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1, queue="priority", preempt=True)
    app.add_arrival(
        "a",
        "constant",
        value=1,
        attributes={
            "p": generator(
                {"gen": "sequence", "start": 3, "step": -1, "loop_after": 4}
            ),
            "can": generator(
                {"gen": "sequence", "start": 1, "step": -1, "loop_after": 2}
            ),
        },
    )

    @app.task(
        name="job",
        resource_id="m",
        priority="attr.p",
        preempt="attr.can",
        service_time=3,
        on_interrupt="restart",
    )
    def job(task_id):
        return {"event_type": "done"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(5):
            entity = yield context.wait_for_arrival("a")
            context.spawn(job(context.next_instance("job"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
