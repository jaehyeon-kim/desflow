"""DS-Y4: An event made now and used later through a local name.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Y4", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_arrival("a", "constant", value=1, first_at=0)

    @app.task(
        name="begin",
        open={
            "slot": {"request": "m"},
            "limit": {"timeout": {"after": 5, "value": "late"}},
            "helper": {"task": "w"},
        },
        next="work",
    )
    def begin(task_id):
        return {"event_type": "begin"}

    @app.task(name="work", service_time=1, next="join")
    def work(task_id):
        return {"event_type": "work"}

    @app.task(
        name="join",
        wait={"any": {"got": {"handle": "slot"}, "out": {"handle": "limit"}}},
        next={
            "when": [{"condition": "handle.helper.alive", "task": "stop"}],
            "default": "end",
        },
    )
    def join(task_id):
        return {"event_type": "join"}

    @app.task(name="stop", interrupt={"handle": "helper", "cause": "stop"}, next="end")
    def stop(task_id):
        return {"event_type": "stop"}

    @app.task(name="end", cancel="slot", release={"handle": "slot"})
    def end(task_id):
        return {"event_type": "end"}

    @app.task(name="w", service_time=9, on_interrupt="abandon")
    def w(task_id):
        return {"event_type": "w"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(2):
            entity = yield context.wait_for_arrival("a")
            context.spawn(begin(context.next_instance("begin"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(on_unhandled_failure="continue")
