"""DS-Y8: A preempt flag and a priority for each request.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template
from desflow.builder.task import NO_RECORD


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Y8", factor=0, random_seed=42)
    app.add_resource("r1", current_cap=1, queue="priority", preempt=True)
    app.add_resource("r2", current_cap=1, queue="priority", preempt=True)
    app.add_resource("m", current_cap=1, queue="priority")
    app.add_resource("n", current_cap=1, queue="priority")
    app.add_arrival("low1", "constant", value=1, first_at=0, attributes={"p": 5})
    app.add_arrival("high1", "constant", value=1, first_at=1, attributes={"p": 1})
    app.add_arrival("low2", "constant", value=1, first_at=0, attributes={"p": 5})
    app.add_arrival("high2", "constant", value=1, first_at=1, attributes={"p": 3})
    app.add_arrival("hm", "constant", value=1, first_at=0)
    app.add_arrival("hn", "constant", value=1, first_at=0)
    app.add_arrival("p", "constant", value=1, first_at=0.5)
    app.add_arrival("q", "constant", value=1, first_at=1)

    @app.task(
        name="job1",
        resource_id="r1",
        priority="attr.p",
        preempt="attr.p < 2",
        service_time=5,
        on_interrupt="abandon",
    )
    def job1(task_id):
        return {"event_type": "done"}

    @app.task(
        name="job2",
        resource_id="r2",
        priority="attr.p",
        preempt="attr.p < 2",
        service_time=5,
        on_interrupt="abandon",
    )
    def job2(task_id):
        return {"event_type": "done"}

    @app.task(name="hold_m", resource_id="m", service_time=2)
    def hold_m(task_id):
        return NO_RECORD

    @app.task(name="hold_n", resource_id="n", service_time=2)
    def hold_n(task_id):
        return NO_RECORD

    ask_payload = Template(
        {"event_type": "got", "at": {"gen": "expr", "expr": "t - 1"}}, name="ask"
    )

    @app.task(
        name="ask",
        wait={
            "all": {
                "x": {"request": {"resource": "m", "priority": 0}},
                "y": {"request": {"resource": "n", "priority": 5}},
            }
        },
        service_time=1,
    )
    def ask(task_id):
        return ask_payload

    only_n_payload = Template(
        {"event_type": "got", "at": {"gen": "expr", "expr": "t - 1"}}, name="only_n"
    )

    @app.task(name="only_n", resource_id="n", priority=1, service_time=1)
    def only_n(task_id):
        return only_n_payload

    @app.arrival_loop("low1")
    def low1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("low1")
            context.spawn(job1(context.next_instance("job1"), entity=entity))

    @app.arrival_loop("high1")
    def high1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("high1")
            context.spawn(job1(context.next_instance("job1"), entity=entity))

    @app.arrival_loop("low2")
    def low2_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("low2")
            context.spawn(job2(context.next_instance("job2"), entity=entity))

    @app.arrival_loop("high2")
    def high2_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("high2")
            context.spawn(job2(context.next_instance("job2"), entity=entity))

    @app.arrival_loop("hm")
    def hm_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("hm")
            context.spawn(hold_m(context.next_instance("hold_m"), entity=entity))

    @app.arrival_loop("hn")
    def hn_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("hn")
            context.spawn(hold_n(context.next_instance("hold_n"), entity=entity))

    @app.arrival_loop("p")
    def p_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("p")
            context.spawn(ask(context.next_instance("ask"), entity=entity))

    @app.arrival_loop("q")
    def q_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("q")
            context.spawn(only_n(context.next_instance("only_n"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
