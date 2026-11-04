"""DS-M5: A custom queue order and preemption rule.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="M5", factor=0, random_seed=42)
    app.add_resource(
        "m1", current_cap=1, queue={"rank": "attr.rank", "ties": "lifo"}, preempt=True
    )
    app.add_resource(
        "m2",
        current_cap=1,
        queue={"rank": "attr.rank"},
        preempt={
            "when": "request.rank < holder.rank and request.key < holder.key "
            "and request.time - holder.since > 1"
        },
    )
    app.add_resource(
        "m3",
        current_cap=1,
        queue={"rank": "attr.rank"},
        preempt={
            "when": "request.rank < holder.rank and request.key < holder.key "
            "and request.time - holder.since > 1"
        },
    )
    app.add_resource(
        "m4",
        current_cap=2,
        queue={"rank": "attr.rank"},
        preempt={"victim": "holder.rank"},
    )
    app.add_arrival(
        "a1",
        "constant",
        value=1,
        first_at=0,
        attributes={"rank": 1, "pre": False, "span": 2},
    )
    app.add_arrival(
        "b1",
        "constant",
        value=1,
        first_at=1,
        attributes={"rank": 1, "pre": False, "span": 1},
    )
    app.add_arrival(
        "c1",
        "constant",
        value=1,
        first_at=1,
        attributes={"rank": 1, "pre": False, "span": 1},
    )
    app.add_arrival(
        "d1",
        "constant",
        value=1,
        first_at=1.5,
        attributes={"rank": 1, "pre": False, "span": 1},
    )
    app.add_arrival(
        "a2",
        "constant",
        value=1,
        first_at=0,
        attributes={"rank": 5, "pre": True, "span": 5},
    )
    app.add_arrival(
        "b2",
        "constant",
        value=1,
        first_at=1,
        attributes={"rank": 1, "pre": True, "span": 1},
    )
    app.add_arrival(
        "a3",
        "constant",
        value=1,
        first_at=0,
        attributes={"rank": 5, "pre": True, "span": 5},
    )
    app.add_arrival(
        "b3",
        "constant",
        value=1,
        first_at=2,
        attributes={"rank": 1, "pre": True, "span": 1},
    )
    app.add_arrival(
        "old",
        "constant",
        value=1,
        first_at=0,
        attributes={"rank": 5, "pre": True, "span": 9},
    )
    app.add_arrival(
        "new",
        "constant",
        value=1,
        first_at=1,
        attributes={"rank": 5, "pre": True, "span": 9},
    )
    app.add_arrival(
        "hi",
        "constant",
        value=1,
        first_at=2,
        attributes={"rank": 1, "pre": True, "span": 9},
    )

    @app.task(
        name="j1",
        resource_id="m1",
        preempt="attr.pre",
        service_time="attr.span",
        on_interrupt="abandon",
    )
    def j1(task_id):
        return {"event_type": "done"}

    @app.task(
        name="j2",
        resource_id="m2",
        preempt="attr.pre",
        service_time="attr.span",
        on_interrupt="abandon",
    )
    def j2(task_id):
        return {"event_type": "done"}

    @app.task(
        name="j3",
        resource_id="m3",
        preempt="attr.pre",
        service_time="attr.span",
        on_interrupt="abandon",
    )
    def j3(task_id):
        return {"event_type": "done"}

    @app.task(
        name="j4",
        resource_id="m4",
        preempt="attr.pre",
        service_time="attr.span",
        on_interrupt="abandon",
    )
    def j4(task_id):
        return {"event_type": "done"}

    @app.arrival_loop("a1")
    def a1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("a1")
            context.spawn(j1(context.next_instance("j1"), entity=entity))

    @app.arrival_loop("b1")
    def b1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("b1")
            context.spawn(j1(context.next_instance("j1"), entity=entity))

    @app.arrival_loop("c1")
    def c1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("c1")
            context.spawn(j1(context.next_instance("j1"), entity=entity))

    @app.arrival_loop("d1")
    def d1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("d1")
            context.spawn(j1(context.next_instance("j1"), entity=entity))

    @app.arrival_loop("a2")
    def a2_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("a2")
            context.spawn(j2(context.next_instance("j2"), entity=entity))

    @app.arrival_loop("b2")
    def b2_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("b2")
            context.spawn(j2(context.next_instance("j2"), entity=entity))

    @app.arrival_loop("a3")
    def a3_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("a3")
            context.spawn(j3(context.next_instance("j3"), entity=entity))

    @app.arrival_loop("b3")
    def b3_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("b3")
            context.spawn(j3(context.next_instance("j3"), entity=entity))

    @app.arrival_loop("old")
    def old_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("old")
            context.spawn(j4(context.next_instance("j4"), entity=entity))

    @app.arrival_loop("new")
    def new_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("new")
            context.spawn(j4(context.next_instance("j4"), entity=entity))

    @app.arrival_loop("hi")
    def hi_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("hi")
            context.spawn(j4(context.next_instance("j4"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=30)
