"""CV-093: A last in, first out store.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_store("s", priority=True)
    app.add_variable("n", 0)
    app.add_arrival("p", "constant", value=1)
    app.add_arrival("g", "constant", value=6)

    @app.task(
        name="put_it",
        put={
            "store": "s",
            "priority": "-registry['C.variables.n']",
            "item": generator({"gen": "expr", "expr": "registry['C.variables.n']"}),
        },
        on=[
            {
                "path": "C.variables.n",
                "value": generator(
                    {"gen": "expr", "expr": "registry['C.variables.n'] + 1"}
                ),
            }
        ],
    )
    def put_it(task_id):
        return {"event_type": "put"}

    get_it_payload = Template(
        {"event_type": "got", "item": {"gen": "expr", "expr": "item"}}, name="get_it"
    )

    @app.task(name="get_it", get={"store": "s"})
    def get_it(task_id):
        return get_it_payload

    @app.arrival_loop("p")
    def p_arrivals(context):
        for _ in range(4):
            entity = yield context.wait_for_arrival("p")
            context.spawn(put_it(context.next_instance("put_it"), entity=entity))

    @app.arrival_loop("g")
    def g_arrivals(context):
        for _ in range(4):
            entity = yield context.wait_for_arrival("g")
            context.spawn(get_it(context.next_instance("get_it"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
