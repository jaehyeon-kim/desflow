"""DX-12: Arguments passed to a child flow and its value read.

Builder form of `model.yaml`.
"""

from desflow import NO_RECORD, SimulationContext
from desflow.values import Template, generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Dock", factor=0)
    app.add_arrival(
        "orders",
        "constant",
        value=1,
        spawn="order",
        max_events=1,
        first_at=0,
        attributes={"boxes": 2},
    )

    @app.task(
        name="order",
        open={
            "p": {
                "task": "pack",
                "args": {"boxes": generator({"gen": "expr", "expr": "attr.boxes"})},
            }
        },
        next="wait_pack",
    )
    def order(task_id):
        return NO_RECORD

    @app.task(
        name="wait_pack",
        wait={"handle": "p"},
        set={"packed": generator({"gen": "expr", "expr": "wait.p.value.total"})},
        next="ship",
    )
    def wait_pack(task_id):
        return NO_RECORD

    @app.task(name="ship")
    def ship(task_id):
        return Template(
            {"total": {"gen": "expr", "expr": "attr.packed + 1"}}, name="tasks.ship"
        )

    @app.task(name="pack", service_time=1)
    def pack(task_id):
        return Template(
            {"total": {"gen": "expr", "expr": "arg.boxes * 3"}}, name="tasks.pack"
        )

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=10)
