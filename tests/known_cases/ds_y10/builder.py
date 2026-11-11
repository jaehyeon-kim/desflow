"""DS-Y10: An event scheduled with a numeric priority.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template
from desflow.builder.task import NO_RECORD


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="Y10", factor=0, random_seed=42)
    app.add_signal("tick_end", once=True)
    app.add_initial_arrival("a", size, spawn="later", attributes={"name": "a"})
    app.add_initial_arrival("m", 1, spawn="mark")
    app.add_initial_arrival("b", 1, spawn="later", attributes={"name": "b"})
    app.add_initial_arrival("w", 1, spawn="see")
    later_payload = Template(
        {"event_type": "later", "name": {"gen": "expr", "expr": "attr.name"}},
        name="later",
    )

    @app.task(name="later", wait={"timeout": 1}, next="again")
    def later(task_id):
        return later_payload

    again_payload = Template(
        {"event_type": "again", "name": {"gen": "expr", "expr": "attr.name"}},
        name="again",
    )

    @app.task(name="again", wait={"timeout": 0})
    def again(task_id):
        return again_payload

    @app.task(
        name="mark", on=[{"fire": {"signal": "tick_end", "priority": 2, "after": 1}}]
    )
    def mark(task_id):
        return NO_RECORD

    @app.task(name="see", wait={"signal": "tick_end"})
    def see(task_id):
        return {"event_type": "marked"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
