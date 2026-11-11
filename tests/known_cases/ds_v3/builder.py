"""DS-V3: Computed values in a write, a cause and a timeout value.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator
from desflow.builder.task import NO_RECORD


def build() -> SimulationContext:
    app = SimulationContext(sim_id="V3", factor=0, random_seed=42)
    app.add_store("built", items=[0, 2, 4])
    app.add_variable("n", 1)
    app.add_variable("total", 10)
    app.add_variable("reason", "r1")
    app.add_arrival("wr", "constant", value=1, first_at=0)
    app.add_arrival("wk", "constant", value=1, first_at=0)
    app.add_arrival("ct", "constant", value=1, first_at=0)
    app.add_arrival("tm", "constant", value=1, first_at=0)
    app.add_arrival("rt", "constant", value=1, first_at=0)

    @app.task(
        name="writes",
        on=[
            {
                "path": "V3.variables.n",
                "value": generator(
                    {"gen": "expr", "expr": "registry['V3.variables.n'] + 1"}
                ),
            },
            {
                "path": "V3.variables.total",
                "value": generator(
                    {"gen": "expr", "expr": "registry['V3.variables.total'] - 3"}
                ),
            },
        ],
        next="writes_report",
    )
    def writes(task_id):
        return NO_RECORD

    writes_report_payload = Template(
        {
            "event_type": "writes",
            "n": {"gen": "expr", "expr": "registry['V3.variables.n']"},
            "total": {"gen": "expr", "expr": "registry['V3.variables.total']"},
            "items": {"gen": "expr", "expr": "items(built)"},
        },
        name="writes_report",
    )

    @app.task(name="writes_report")
    def writes_report(task_id):
        return writes_report_payload

    @app.task(
        name="worker",
        wait={"timeout": 9},
        on_interrupt="abandon",
        on_interrupt_next="worker_cause",
    )
    def worker(task_id):
        return NO_RECORD

    worker_cause_payload = Template(
        {"event_type": "cause", "cause": {"gen": "expr", "expr": "cause.value"}},
        name="worker_cause",
    )

    @app.task(name="worker_cause")
    def worker_cause(task_id):
        return worker_cause_payload

    @app.task(
        name="cutter",
        wait={"timeout": 1},
        on=[
            {
                "interrupt": {
                    "task": "worker",
                    "cause": generator(
                        {"gen": "expr", "expr": "registry['V3.variables.reason']"}
                    ),
                }
            },
            {"path": "V3.variables.reason", "value": "changed in the same step"},
        ],
    )
    def cutter(task_id):
        return {"event_type": "cutter_goes_on_first"}

    @app.task(
        name="timed",
        set={"made": generator({"gen": "expr", "expr": "t"})},
        next="timed1",
    )
    def timed(task_id):
        return NO_RECORD

    timed1_payload = Template(
        {
            "event_type": "value_fixed",
            "v": {"gen": "expr", "expr": "wait.timeout.value"},
            "made": {"gen": "expr", "expr": "attr.made"},
            "now": {"gen": "expr", "expr": "t"},
        },
        name="timed1",
    )

    @app.task(
        name="timed1",
        wait={
            "timeout": {
                "after": 2,
                "value": generator({"gen": "expr", "expr": "t * 2"}),
            }
        },
        next="timed_mark",
    )
    def timed1(task_id):
        return timed1_payload

    @app.task(
        name="timed_mark",
        set={"made": generator({"gen": "expr", "expr": "t"})},
        next="timed2",
    )
    def timed_mark(task_id):
        return NO_RECORD

    timed2_payload = Template(
        {
            "event_type": "value_fixed",
            "v": {"gen": "expr", "expr": "wait.timeout.value"},
            "made": {"gen": "expr", "expr": "attr.made"},
            "now": {"gen": "expr", "expr": "t"},
        },
        name="timed2",
    )

    @app.task(
        name="timed2",
        wait={
            "timeout": {
                "after": 2,
                "value": generator({"gen": "expr", "expr": "t * 2"}),
            }
        },
    )
    def timed2(task_id):
        return timed2_payload

    @app.task(
        name="parent",
        wait={"task": "child"},
        set={"plain": generator({"gen": "expr", "expr": "wait.child.value.v"})},
        next="parent2",
    )
    def parent(task_id):
        return NO_RECORD

    parent2_payload = Template(
        {
            "event_type": "returned",
            "plain": {"gen": "attr", "name": "plain"},
            "field": {"gen": "expr", "expr": "wait.child.value.v"},
            "same": {"gen": "expr", "expr": "attr.plain == wait.child.value.v"},
        },
        name="parent2",
    )

    @app.task(name="parent2", wait={"task": "child"})
    def parent2(task_id):
        return parent2_payload

    @app.task(name="child", wait={"timeout": 1})
    def child(task_id):
        return {"v": 10}

    @app.arrival_loop("wr")
    def wr_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("wr")
            context.spawn(writes(context.next_instance("writes"), entity=entity))

    @app.arrival_loop("wk")
    def wk_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("wk")
            context.spawn(worker(context.next_instance("worker"), entity=entity))

    @app.arrival_loop("ct")
    def ct_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("ct")
            context.spawn(cutter(context.next_instance("cutter"), entity=entity))

    @app.arrival_loop("tm")
    def tm_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("tm")
            context.spawn(timed(context.next_instance("timed"), entity=entity))

    @app.arrival_loop("rt")
    def rt_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("rt")
            context.spawn(parent(context.next_instance("parent"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
