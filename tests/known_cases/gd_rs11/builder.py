"""GD-RS11: A preemptive resource with its own queue or preemption rule.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="R", factor=0, random_seed=42)
    app.add_resource(
        "res",
        current_cap=1,
        queue={"rank": "attr.deadline - t", "ties": "fifo"},
        preempt={"when": "request.rank < holder.rank", "victim": "holder.rank"},
    )
    app.add_arrival(
        "a",
        "constant",
        value=1,
        first_at=0,
        attributes={
            "deadline": generator({"gen": "sequence", "start": 20, "step": -3})
        },
    )

    @app.task(name="job", resource_id="res", service_time=3)
    def job(task_id):
        return {"event_type": "done"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(4):
            entity = yield context.wait_for_arrival("a")
            context.spawn(job(context.next_instance("job"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
