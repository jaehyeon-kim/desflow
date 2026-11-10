"""DS-V7: Names for a cause, holders, an item priority and nested values.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template
from desflow.core.task import NO_RECORD


def build() -> SimulationContext:
    app = SimulationContext(sim_id="V7", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1, queue="priority", preempt=True)
    app.add_resource("m2", current_cap=2)
    app.add_store("q", priority=True)
    app.add_arrival("low", "constant", value=1, first_at=0, spawn="low", max_events=1)
    app.add_arrival("high", "constant", value=1, first_at=0, spawn="high", max_events=1)
    app.add_arrival("a", "constant", value=1, first_at=0, spawn="hold2", max_events=1)
    app.add_arrival("b", "constant", value=1, first_at=0, spawn="hold2", max_events=1)
    app.add_arrival("pr", "constant", value=1, first_at=0, spawn="put1", max_events=1)
    app.add_arrival("fin", "constant", value=1, first_at=20, spawn="end", max_events=1)

    @app.task(
        name="low",
        resource_id="m",
        priority=5,
        service_time=10,
        on_interrupt="abandon",
        on_interrupt_next="low_cause",
    )
    def low(task_id):
        return NO_RECORD

    low_cause_payload = Template(
        {
            "event_type": "cause",
            "by": {"gen": "expr", "expr": "cause.by"},
            "usage_since": {"gen": "expr", "expr": "cause.usage_since"},
            "left": {"gen": "expr", "expr": "cause.left"},
            "state": {"gen": "expr", "expr": "upstream.status"},
            "now": {"gen": "expr", "expr": "t"},
        },
        name="tasks.low_cause",
    )

    @app.task(name="low_cause")
    def low_cause(task_id):
        return low_cause_payload

    @app.task(name="high", wait={"timeout": 0}, next="high_seize")
    def high(task_id):
        return NO_RECORD

    high_seize_payload = Template(
        {
            "event_type": "high_granted",
            "holders": {"gen": "expr", "expr": "len(held(m))"},
            "at": {"gen": "expr", "expr": "t"},
        },
        name="tasks.high_seize",
    )

    @app.task(name="high_seize", seize="m", release="m")
    def high_seize(task_id):
        return high_seize_payload

    end_payload = Template(
        {
            "event_type": "holders_at_the_end",
            "holders": {"gen": "expr", "expr": "len(held(m))"},
        },
        name="tasks.end",
    )

    @app.task(name="end")
    def end(task_id):
        return end_payload

    hold2_payload = Template(
        {
            "event_type": "users",
            "who": {"gen": "expr", "expr": "[h.entity for h in held(m2)]"},
        },
        name="tasks.hold2",
    )

    @app.task(name="hold2", seize="m2", next="hold2_end")
    def hold2(task_id):
        return hold2_payload

    @app.task(name="hold2_end", service_time=5, release="m2")
    def hold2_end(task_id):
        return NO_RECORD

    @app.task(name="put1", put={"store": "q", "priority": 2}, next="put2")
    def put1(task_id):
        return {"req": {"id": 7}}

    @app.task(name="put2", put={"store": "q", "priority": 1}, next="get1")
    def put2(task_id):
        return {"req": {"id": 8}}

    get1_payload = Template(
        {
            "event_type": "got",
            "prio": {"gen": "expr", "expr": "item_priority"},
            "id": {"gen": "expr", "expr": "item.req.id"},
        },
        name="tasks.get1",
    )

    @app.task(name="get1", get={"store": "q"}, next="get2")
    def get1(task_id):
        return get1_payload

    get2_payload = Template(
        {
            "event_type": "got",
            "prio": {"gen": "expr", "expr": "item_priority"},
            "id": {"gen": "expr", "expr": "item.req.id"},
        },
        name="tasks.get2",
    )

    @app.task(name="get2", get={"store": "q"})
    def get2(task_id):
        return get2_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
