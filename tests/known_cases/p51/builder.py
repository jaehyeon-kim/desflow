"""P51: Setup or changeover.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_variable("last_kind", "none")
    app.add_arrival(
        "a",
        "constant",
        value=1,
        attributes={"kind": generator({"gen": "one_of", "choices": ["a", "b"]})},
    )

    @app.task(
        name="job",
        resource_id="m",
        service_time="(2 if registry['P.variables.last_kind'] != attr.kind else 0) + 3",
        on=[
            {
                "path": "P.variables.last_kind",
                "value": generator({"gen": "expr", "expr": "attr.kind"}),
            }
        ],
    )
    def job(task_id):
        return {"event_type": "done"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(8):
            entity = yield context.wait_for_arrival("a")
            context.spawn(job(context.next_instance("job"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
