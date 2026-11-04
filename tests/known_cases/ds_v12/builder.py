"""DS-V12: Put back the item a get took.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="V12", factor=0, random_seed=42)
    app.add_store("tools", items=[{"req": {"id": 7}}, {"req": {"id": 8}}, 5, 0])
    app.add_store("plan", items=[{"d": 3}])
    app.add_arrival("p", "constant", value=3, first_at=0)
    app.add_arrival("q", "constant", value=3, first_at=0)
    take_payload = Template(
        {"event_type": "took", "item": {"gen": "expr", "expr": "item"}}, name="take"
    )

    @app.task(
        name="take",
        get={"store": "tools", "filter": "item.req.id == 7"},
        set={"kept": generator({"gen": "expr", "expr": "item"})},
        next="second",
    )
    def take(task_id):
        return take_payload

    second_payload = Template(
        {"event_type": "second", "item": {"gen": "expr", "expr": "item"}}, name="second"
    )

    @app.task(
        name="second", get={"store": "tools", "filter": "item == 5"}, next="third"
    )
    def second(task_id):
        return second_payload

    third_payload = Template(
        {"event_type": "third", "item": {"gen": "expr", "expr": "item"}}, name="third"
    )

    @app.task(name="third", get={"store": "tools", "filter": "item == 0"}, next="back")
    def third(task_id):
        return third_payload

    @app.task(
        name="back",
        put={"store": "tools", "item": generator({"gen": "attr", "name": "kept"})},
        next="num",
    )
    def back(task_id):
        return {"event_type": "back"}

    @app.task(name="num", put={"store": "tools", "item": 5}, next="report")
    def num(task_id):
        return {"event_type": "num"}

    report_payload = Template(
        {"event_type": "items", "items": {"gen": "expr", "expr": "items(tools)"}},
        name="report",
    )

    @app.task(name="report")
    def report(task_id):
        return report_payload

    after_get_payload = Template(
        {"event_type": "used", "at": {"gen": "expr", "expr": "t"}}, name="after_get"
    )

    @app.task(name="after_get", get={"store": "plan"}, service_time="item.d")
    def after_get(task_id):
        return after_get_payload

    @app.arrival_loop("p")
    def p_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("p")
            context.spawn(take(context.next_instance("take"), entity=entity))

    @app.arrival_loop("q")
    def q_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("q")
            context.spawn(after_get(context.next_instance("after_get"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
