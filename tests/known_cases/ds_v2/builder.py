"""DS-V2: A wait timeout computed for each entity.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator
from desflow.builder.task import NO_RECORD


def build() -> SimulationContext:
    app = SimulationContext(sim_id="V2", factor=0, random_seed=42)
    app.add_signal("e1", once=True)
    app.add_signal("e2", once=True)
    app.add_arrival(
        "w1",
        "constant",
        value=1,
        first_at=0,
        attributes={"limit": generator({"gen": "integer", "bounds": [3, 7]})},
    )
    app.add_arrival("s1", "constant", value=1, first_at=0)
    app.add_arrival(
        "w2",
        "constant",
        value=1,
        first_at=0,
        attributes={"limit": generator({"gen": "integer", "bounds": [1, 3]})},
    )
    app.add_arrival("s2", "constant", value=1, first_at=0)
    waiter_payload = Template(
        {
            "event_type": "left",
            "ev": {"gen": "expr", "expr": "wait.ev.done"},
            "limit": {"gen": "expr", "expr": "wait.limit.done"},
        },
        name="waiter",
    )

    @app.task(
        name="waiter",
        wait={"any": {"ev": {"signal": "e1"}, "limit": {"timeout": "attr.limit"}}},
    )
    def waiter(task_id):
        return waiter_payload

    waiter2_payload = Template(
        {
            "event_type": "left",
            "ev": {"gen": "expr", "expr": "wait.ev.done"},
            "limit": {"gen": "expr", "expr": "wait.limit.done"},
        },
        name="waiter2",
    )

    @app.task(
        name="waiter2",
        wait={"any": {"ev": {"signal": "e2"}, "limit": {"timeout": "attr.limit"}}},
    )
    def waiter2(task_id):
        return waiter2_payload

    @app.task(
        name="send1", wait={"timeout": 1}, on=[{"fire": {"signal": "e1", "value": 1}}]
    )
    def send1(task_id):
        return NO_RECORD

    @app.task(
        name="send2", wait={"timeout": 5}, on=[{"fire": {"signal": "e2", "value": 1}}]
    )
    def send2(task_id):
        return NO_RECORD

    @app.arrival_loop("w1")
    def w1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("w1")
            context.spawn(waiter(context.next_instance("waiter"), entity=entity))

    @app.arrival_loop("s1")
    def s1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("s1")
            context.spawn(send1(context.next_instance("send1"), entity=entity))

    @app.arrival_loop("w2")
    def w2_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("w2")
            context.spawn(waiter2(context.next_instance("waiter2"), entity=entity))

    @app.arrival_loop("s2")
    def s2_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("s2")
            context.spawn(send2(context.next_instance("send2"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
