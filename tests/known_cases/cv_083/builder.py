"""CV-083: A resource with a grant rule of its own.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_variable("level", 0)
    app.add_variable("tickets", 0)
    app.add_variable("turn", 0)
    app.add_initial_arrival("g0", size, spawn="ticket", attributes={"amount": 3})
    app.add_initial_arrival("g1", 1, spawn="ticket", attributes={"amount": 1})
    app.add_initial_arrival("g2", 1, spawn="ticket", attributes={"amount": 4})
    app.add_arrival("p1", "constant", value=1, attributes={"amount": 2})
    app.add_arrival("p2", "constant", value=2, attributes={"amount": 5})

    @app.task(
        name="ticket",
        set={
            "i": generator({"gen": "expr", "expr": "registry['C.variables.tickets']"})
        },
        on=[
            {
                "path": "C.variables.tickets",
                "value": generator(
                    {"gen": "expr", "expr": "registry['C.variables.tickets'] + 1"}
                ),
            }
        ],
        next="get_it",
    )
    def ticket(task_id):
        return {"event_type": "ticket"}

    get_it_payload = Template(
        {
            "event_type": "got",
            "taken": {
                "gen": "expr",
                "expr": "min(registry['C.variables.level'], attr.amount)",
            },
        },
        name="get_it",
    )

    @app.task(
        name="get_it",
        wait={
            "condition": "registry['C.variables.level'] >= 1 and registry['C.variables.turn'] == attr.i"
        },
        on=[
            {
                "path": "C.variables.level",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "registry['C.variables.level'] - min(registry['C.variables.level'], attr.amount)",
                    }
                ),
            },
            {
                "path": "C.variables.turn",
                "value": generator(
                    {"gen": "expr", "expr": "registry['C.variables.turn'] + 1"}
                ),
            },
        ],
    )
    def get_it(task_id):
        return get_it_payload

    @app.task(
        name="put_it",
        on=[
            {
                "path": "C.variables.level",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "registry['C.variables.level'] + attr.amount",
                    }
                ),
            }
        ],
    )
    def put_it(task_id):
        return {"event_type": "put"}

    @app.arrival_loop("p1")
    def p1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("p1")
            context.spawn(put_it(context.next_instance("put_it"), entity=entity))

    @app.arrival_loop("p2")
    def p2_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("p2")
            context.spawn(put_it(context.next_instance("put_it"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
