"""DS-Y7: Names for another process, an event state and a queue's entries.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template
from desflow.builder.task import NO_RECORD


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Y7", factor=0, random_seed=42)
    app.add_resource("r", current_cap=1, queue="priority")
    app.add_container("s", current_cap=5, level=0)
    app.add_signal("ev", once=True)
    app.add_arrival("c", "constant", value=1, first_at=0)
    app.add_arrival("p", "constant", value=1, first_at=0)
    app.add_arrival("a", "constant", value=1, first_at=0)
    app.add_arrival(
        "j1", "constant", value=1, first_at=0, attributes={"prio": 1, "at": 0}
    )
    app.add_arrival(
        "j2", "constant", value=1, first_at=0, attributes={"prio": 3, "at": 0.5}
    )
    app.add_arrival(
        "j3", "constant", value=1, first_at=0, attributes={"prio": 2, "at": 1}
    )
    app.add_arrival("g1", "constant", value=1, first_at=0, attributes={"amount": 2})
    app.add_arrival("g2", "constant", value=1, first_at=0, attributes={"amount": 3})
    app.add_arrival("lk", "constant", value=1, first_at=2)
    app.add_arrival("pk", "constant", value=1, first_at=100)

    @app.task(name="w", wait={"timeout": 2})
    def w(task_id):
        return NO_RECORD

    watch_payload = Template(
        {"event_type": "alive", "alive": {"gen": "expr", "expr": "running(w) > 0"}},
        name="watch",
    )

    @app.task(name="watch", next="watch2")
    def watch(task_id):
        return watch_payload

    watch2_payload = Template(
        {"event_type": "alive", "alive": {"gen": "expr", "expr": "running(w) > 0"}},
        name="watch2",
    )

    @app.task(name="watch2", wait={"timeout": 1}, next="watch3")
    def watch2(task_id):
        return watch2_payload

    watch3_payload = Template(
        {"event_type": "alive", "alive": {"gen": "expr", "expr": "running(w) > 0"}},
        name="watch3",
    )

    @app.task(name="watch3", wait={"timeout": 1})
    def watch3(task_id):
        return watch3_payload

    states_payload = Template(
        {
            "event_type": "before",
            "triggered": {"gen": "expr", "expr": "signal.ev.count > 0"},
            "processed": {"gen": "expr", "expr": "signal.ev.processed"},
        },
        name="states",
    )

    @app.task(
        name="states", on=[{"fire": {"signal": "ev", "value": 1}}], next="states_fired"
    )
    def states(task_id):
        return states_payload

    states_fired_payload = Template(
        {
            "event_type": "fired",
            "triggered": {"gen": "expr", "expr": "signal.ev.count > 0"},
            "processed": {"gen": "expr", "expr": "signal.ev.processed"},
        },
        name="states_fired",
    )

    @app.task(name="states_fired", next="states_after")
    def states_fired(task_id):
        return states_fired_payload

    states_after_payload = Template(
        {
            "event_type": "after",
            "triggered": {"gen": "expr", "expr": "signal.ev.count > 0"},
            "processed": {"gen": "expr", "expr": "signal.ev.processed"},
        },
        name="states_after",
    )

    @app.task(name="states_after", wait={"timeout": 0})
    def states_after(task_id):
        return states_after_payload

    @app.task(name="j", wait={"timeout": "attr.at"}, next="j_hold")
    def j(task_id):
        return NO_RECORD

    @app.task(name="j_hold", resource_id="r", priority="attr.prio", service_time=5)
    def j_hold(task_id):
        return NO_RECORD

    @app.task(name="g", get={"container": "s", "amount": "attr.amount"})
    def g(task_id):
        return NO_RECORD

    look_payload = Template(
        {
            "event_type": "look",
            "users": {"gen": "expr", "expr": "[[h.rank, h.since] for h in held(r)]"},
            "queue": {"gen": "expr", "expr": "[[w.rank, w.time] for w in waiting(r)]"},
            "get_queue": {"gen": "expr", "expr": "[g.amount for g in getting(s)]"},
        },
        name="look",
    )

    @app.task(name="look")
    def look(task_id):
        return look_payload

    peeker_payload = Template(
        {
            "event_type": "peek",
            "next_event": {"gen": "expr", "expr": "peek()"},
            "at": {"gen": "expr", "expr": "t"},
        },
        name="peeker",
    )

    @app.task(name="peeker", open={"t3": {"timeout": {"after": 3}}}, next="peeker2")
    def peeker(task_id):
        return peeker_payload

    peeker2_payload = Template(
        {
            "event_type": "peek",
            "next_event": {"gen": "expr", "expr": "peek()"},
            "at": {"gen": "expr", "expr": "t"},
        },
        name="peeker2",
    )

    @app.task(name="peeker2", wait={"timeout": 1})
    def peeker2(task_id):
        return peeker2_payload

    @app.arrival_loop("c")
    def c_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("c")
            context.spawn(w(context.next_instance("w"), entity=entity))

    @app.arrival_loop("p")
    def p_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("p")
            context.spawn(watch(context.next_instance("watch"), entity=entity))

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("a")
            context.spawn(states(context.next_instance("states"), entity=entity))

    @app.arrival_loop("j1")
    def j1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("j1")
            context.spawn(j(context.next_instance("j"), entity=entity))

    @app.arrival_loop("j2")
    def j2_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("j2")
            context.spawn(j(context.next_instance("j"), entity=entity))

    @app.arrival_loop("j3")
    def j3_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("j3")
            context.spawn(j(context.next_instance("j"), entity=entity))

    @app.arrival_loop("g1")
    def g1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("g1")
            context.spawn(g(context.next_instance("g"), entity=entity))

    @app.arrival_loop("g2")
    def g2_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("g2")
            context.spawn(g(context.next_instance("g"), entity=entity))

    @app.arrival_loop("lk")
    def lk_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("lk")
            context.spawn(look(context.next_instance("look"), entity=entity))

    @app.arrival_loop("pk")
    def pk_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("pk")
            context.spawn(peeker(context.next_instance("peeker"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
