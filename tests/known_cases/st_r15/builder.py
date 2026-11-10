"""ST-R15: Nested preemption over two resources.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_resource("res0", current_cap=1, queue="priority", preempt=True)
    app.add_resource("res1", current_cap=1, queue="priority", preempt=True)
    app.add_initial_arrival("p0", size, spawn="two_first", attributes={"prio": -1})
    app.add_arrival("p1", "constant", value=1, attributes={"prio": -2})
    app.add_arrival("p2", "constant", value=20, attributes={"prio": -1})
    app.add_arrival("p3", "constant", value=21, attributes={"prio": -2})
    app.add_arrival("p4", "constant", value=21, attributes={"prio": -1})

    @app.task(
        name="two_first",
        seize={"resource": "res0", "priority": "attr.prio", "preempt": True},
        on_interrupt="abandon",
        on_interrupt_next="lost",
        next="two_second",
    )
    def two_first(task_id):
        return {"event_type": "got_res0"}

    @app.task(
        name="two_second",
        seize={"resource": "res1", "priority": "attr.prio", "preempt": True},
        service_time=5,
        on_interrupt="abandon",
        on_interrupt_next="lost",
        release="res1",
        next="two_done",
    )
    def two_second(task_id):
        return {"event_type": "done"}

    @app.task(name="two_done", release="res0")
    def two_done(task_id):
        return {"event_type": "freed_res0"}

    @app.task(
        name="one",
        resource_id="res1",
        priority="attr.prio",
        preempt=True,
        service_time=5,
        on_interrupt="abandon",
    )
    def one(task_id):
        return {"event_type": "done"}

    @app.task(
        name="zero",
        resource_id="res0",
        priority="attr.prio",
        preempt=True,
        service_time=5,
        on_interrupt="abandon",
    )
    def zero(task_id):
        return {"event_type": "done"}

    lost_payload = Template(
        {
            "event_type": "preempted",
            "by": {"gen": "expr", "expr": "cause.by"},
            "since": {"gen": "expr", "expr": "cause.usage_since"},
            "resource": {"gen": "expr", "expr": "cause.resource"},
        },
        name="lost",
    )

    @app.task(name="lost", release="res1", next="lost_free_res0")
    def lost(task_id):
        return lost_payload

    @app.task(name="lost_free_res0", release="res0")
    def lost_free_res0(task_id):
        return {"event_type": "freed_after_preemption"}

    @app.arrival_loop("p1")
    def p1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("p1")
            context.spawn(one(context.next_instance("one"), entity=entity))

    @app.arrival_loop("p2")
    def p2_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("p2")
            context.spawn(two_first(context.next_instance("two_first"), entity=entity))

    @app.arrival_loop("p3")
    def p3_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("p3")
            context.spawn(zero(context.next_instance("zero"), entity=entity))

    @app.arrival_loop("p4")
    def p4_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("p4")
            context.spawn(two_first(context.next_instance("two_first"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
