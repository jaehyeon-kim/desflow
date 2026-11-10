"""P45: Jockeying between two lines.

Builder form of `model.yaml`.
"""

from desflow import NO_RECORD, SimulationContext


def build(size: int = 3) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("a", current_cap=1)
    app.add_resource("b", current_cap=1)
    app.add_service("s", dist="constant", value=4)
    app.add_initial_arrival("c", size, spawn="t", attributes={"leaving": 0})

    @app.task(name="t", wait={"all": {"q": {"task": "qa"}, "tm": {"task": "timer"}}})
    def t(task_id):
        return NO_RECORD

    @app.task(
        name="qa",
        resource_id="a",
        service_id="s",
        on_interrupt="abandon",
        on_interrupt_next="qb",
    )
    def qa(task_id):
        return {"event_type": "served_a"}

    @app.task(name="qb", resource_id="b", service_id="s")
    def qb(task_id):
        return {"event_type": "served_b"}

    @app.task(
        name="timer",
        service_time=1,
        next={"when": [{"condition": "users.b + queue.b < queue.a", "task": "mark"}]},
    )
    def timer(task_id):
        return NO_RECORD

    @app.task(name="mark", set={"leaving": 1}, next="cut")
    def mark(task_id):
        return NO_RECORD

    @app.task(
        name="cut",
        interrupt={
            "task": "qa",
            "where": "attr.leaving == 1 and process.target == 'resource.a'",
        },
        next="unmark",
    )
    def cut(task_id):
        return NO_RECORD

    @app.task(name="unmark", set={"leaving": 0})
    def unmark(task_id):
        return NO_RECORD

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
