"""DS-Y6: A local variable that two children change at once.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Y6", factor=0, random_seed=42)
    app.add_arrival("a", "constant", value=1, first_at=0)

    @app.task(
        name="parent",
        wait={
            "all": {
                "x": {"task": "count", "args": {"gap": 1}},
                "y": {"task": "count", "args": {"gap": 1.5}},
            }
        },
    )
    def parent(task_id):
        return {"event_type": "parent"}

    @app.task(name="count", set_args={"i": 0}, next="step")
    def count(task_id):
        return {"event_type": "begin"}

    step_payload = Template(
        {"event_type": "step", "n": {"gen": "expr", "expr": "arg.i + 1"}}, name="step"
    )

    @app.task(
        name="step",
        service_time="arg.gap",
        set_args={"i": generator({"gen": "expr", "expr": "arg.i + 1"})},
        next={"when": [{"condition": "arg.i < 2", "task": "step"}]},
    )
    def step(task_id):
        return step_payload

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("a")
            context.spawn(parent(context.next_instance("parent"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
