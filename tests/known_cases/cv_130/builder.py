"""CV-130: A generator function picked by a computed name.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_arrival(
        "a",
        "constant",
        value=1,
        attributes={"which": generator({"gen": "one_of", "choices": ["a", "b"]})},
    )

    @app.task(
        name="pick",
        next={
            "when": [{"condition": "attr.which == 'a'", "task": "run_a"}],
            "default": "run_b",
        },
    )
    def pick(task_id):
        return {"event_type": "pick"}

    @app.task(name="run_a", spawn={"task": "proc_a"})
    def run_a(task_id):
        return {"event_type": "run_a"}

    @app.task(name="run_b", spawn={"task": "proc_b"})
    def run_b(task_id):
        return {"event_type": "run_b"}

    @app.task(name="proc_a", service_time=1)
    def proc_a(task_id):
        return {"event_type": "proc_a"}

    @app.task(name="proc_b", service_time=2)
    def proc_b(task_id):
        return {"event_type": "proc_b"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(4):
            entity = yield context.wait_for_arrival("a")
            context.spawn(pick(context.next_instance("pick"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
