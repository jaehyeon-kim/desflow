"""P34: A capacity computed from the state.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1, max_cap=3)
    app.add_service("s", dist="constant", value=5)
    app.add_arrival("a", "constant", value=1, first_at=0)
    app.add_arrival("boss", "constant", value=2)

    @app.task(name="job", resource_id="m", service_id="s")
    def job(task_id):
        return {"event_type": "done"}

    @app.task(
        name="grow",
        on=[
            {
                "path": "P.resources.m.current_cap",
                "value": generator({"gen": "expr", "expr": "queue.m + 1"}),
            }
        ],
    )
    def grow(task_id):
        return {"event_type": "grow"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(2):
            entity = yield context.wait_for_arrival("a")
            context.spawn(job(context.next_instance("job"), entity=entity))

    @app.arrival_loop("boss")
    def boss_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("boss")
            context.spawn(grow(context.next_instance("grow"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
