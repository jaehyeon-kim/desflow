"""DS-I6: Go on after reneging or balking.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template
from desflow.builder.task import NO_RECORD


def build() -> SimulationContext:
    app = SimulationContext(sim_id="I6", factor=0, random_seed=42)
    app.add_resource("m1", current_cap=1)
    app.add_resource("m2", current_cap=1)
    app.add_resource("m3", current_cap=1)
    app.add_arrival("h1", "constant", value=1, first_at=0)
    app.add_arrival("l1", "constant", value=1, first_at=1)
    app.add_arrival("h2", "constant", value=1, first_at=0)
    app.add_arrival("l2", "constant", value=1, first_at=1)
    app.add_arrival("h3", "constant", value=1, first_at=0)
    app.add_arrival("l3", "constant", value=1, first_at=1)

    @app.task(name="hold1", resource_id="m1", service_time=5)
    def hold1(task_id):
        return NO_RECORD

    @app.task(name="hold2", resource_id="m2", service_time=5)
    def hold2(task_id):
        return NO_RECORD

    @app.task(name="hold3", resource_id="m3", service_time=5)
    def hold3(task_id):
        return NO_RECORD

    @app.task(
        name="late1",
        patience=2,
        resource_id="m1",
        service_time=0,
        on_renege="elsewhere",
    )
    def late1(task_id):
        return {"event_type": "served"}

    @app.task(
        name="late2",
        patience=4,
        resource_id="m2",
        service_time=0,
        on_renege="elsewhere",
    )
    def late2(task_id):
        return {"event_type": "served"}

    @app.task(
        name="late3",
        balk="queue.m3 >= 0",
        patience=2,
        resource_id="m3",
        service_time=0,
        on_balk="elsewhere",
    )
    def late3(task_id):
        return {"event_type": "served"}

    elsewhere_payload = Template(
        {"event_type": "elsewhere", "why": {"gen": "expr", "expr": "upstream.status"}},
        name="elsewhere",
    )

    @app.task(name="elsewhere")
    def elsewhere(task_id):
        return elsewhere_payload

    @app.arrival_loop("h1")
    def h1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("h1")
            context.spawn(hold1(context.next_instance("hold1"), entity=entity))

    @app.arrival_loop("l1")
    def l1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("l1")
            context.spawn(late1(context.next_instance("late1"), entity=entity))

    @app.arrival_loop("h2")
    def h2_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("h2")
            context.spawn(hold2(context.next_instance("hold2"), entity=entity))

    @app.arrival_loop("l2")
    def l2_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("l2")
            context.spawn(late2(context.next_instance("late2"), entity=entity))

    @app.arrival_loop("h3")
    def h3_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("h3")
            context.spawn(hold3(context.next_instance("hold3"), entity=entity))

    @app.arrival_loop("l3")
    def l3_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("l3")
            context.spawn(late3(context.next_instance("late3"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
