"""DS-V9: Arguments for a child flow.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator
from desflow.core.task import NO_RECORD


def build() -> SimulationContext:
    app = SimulationContext(sim_id="V9", factor=0, random_seed=42)
    app.add_arrival("a", "constant", value=4, first_at=0, attributes={"k": 2})

    @app.task(
        name="parent",
        open={
            "a": {
                "task": "child",
                "args": {"x": generator({"gen": "expr", "expr": "attr.k * 3"})},
            }
        },
        set={"k": 100},
        next="start_b",
    )
    def parent(task_id):
        return NO_RECORD

    @app.task(
        name="start_b",
        open={
            "b": {
                "task": "child",
                "args": {"x": generator({"gen": "expr", "expr": "attr.k * 3"})},
            }
        },
        next="wait_a",
    )
    def start_b(task_id):
        return {"event_type": "parent_goes_on_first"}

    @app.task(
        name="wait_a",
        wait={"handle": "a"},
        set={"va": generator({"gen": "expr", "expr": "wait.a.value.v"})},
        next="wait_b",
    )
    def wait_a(task_id):
        return NO_RECORD

    wait_b_payload = Template(
        {
            "event_type": "results",
            "va": {"gen": "attr", "name": "va"},
            "vb": {"gen": "expr", "expr": "wait.b.value.v"},
        },
        name="wait_b",
    )

    @app.task(name="wait_b", wait={"handle": "b"})
    def wait_b(task_id):
        return wait_b_payload

    child_payload = Template(
        {"event_type": "starts", "x": {"gen": "expr", "expr": "arg.x"}}, name="child"
    )

    @app.task(name="child", next="child_end")
    def child(task_id):
        return child_payload

    child_end_payload = Template(
        {"event_type": "ended", "v": {"gen": "expr", "expr": "arg.x + 1"}},
        name="child_end",
    )

    @app.task(name="child_end", service_time=1)
    def child_end(task_id):
        return child_end_payload

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("a")
            context.spawn(parent(context.next_instance("parent"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
