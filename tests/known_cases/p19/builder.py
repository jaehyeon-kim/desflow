"""P19: A global counter that is not a plain increment.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_container("total", level=0)
    app.add_container("lock", current_cap=1, level=1)
    app.add_arrival("a", "constant", value=5, attributes={"k": 3})

    @app.task(name="add", get={"container": "lock", "amount": 1}, next="put_total")
    def add(task_id):
        return {"event_type": "locked"}

    @app.task(
        name="put_total",
        put={"container": "total", "amount": "attr.k * 2"},
        next={
            "when": [{"condition": "level.total >= 10", "task": "big"}],
            "default": "small",
        },
    )
    def put_total(task_id):
        return {"event_type": "added"}

    @app.task(name="big", put={"container": "lock", "amount": 1})
    def big(task_id):
        return {"event_type": "big"}

    @app.task(name="small", put={"container": "lock", "amount": 1})
    def small(task_id):
        return {"event_type": "small"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(4):
            entity = yield context.wait_for_arrival("a")
            context.spawn(add(context.next_instance("add"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
