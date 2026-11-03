"""GD-RS10: A priority store with text priorities.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="R", factor=0, random_seed=42)
    app.add_store("repairs", priority=True)
    app.add_initial_arrival("inspector", size, spawn="tick", attributes={"n": 0})
    app.add_initial_arrival("maintainer", 1, spawn="wait3")
    tick_payload = Template(
        {
            "event_type": "log",
            "priority": {"gen": "expr", "expr": "['P2', 'P0', 'P3', 'P1'][attr.n]"},
            "item": {
                "gen": "expr",
                "expr": "['#0000', '#0001', '#0002', '#0003'][attr.n]",
            },
        },
        name="tick",
    )

    @app.task(name="tick", service_time=1, next="file")
    def tick(task_id):
        return tick_payload

    @app.task(
        name="file",
        put={
            "store": "repairs",
            "priority": "['P2', 'P0', 'P3', 'P1'][attr.n]",
            "item": {
                "prio": generator(
                    {"gen": "expr", "expr": "['P2', 'P0', 'P3', 'P1'][attr.n]"}
                ),
                "issue": generator(
                    {
                        "gen": "expr",
                        "expr": "['#0000', '#0001', '#0002', '#0003'][attr.n]",
                    }
                ),
            },
        },
        set={"n": generator({"gen": "expr", "expr": "attr.n + 1"})},
        next={"when": [{"condition": "attr.n < 4", "task": "tick"}]},
    )
    def file(task_id):
        return {"event_type": "filed"}

    @app.task(name="wait3", service_time=3, next="take")
    def wait3(task_id):
        return {"event_type": "waited"}

    take_payload = Template(
        {
            "event_type": "repair",
            "priority": {"gen": "expr", "expr": "item.prio"},
            "item": {"gen": "expr", "expr": "item.issue"},
        },
        name="take",
    )

    @app.task(name="take", get={"store": "repairs"}, next="wait3")
    def take(task_id):
        return take_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
