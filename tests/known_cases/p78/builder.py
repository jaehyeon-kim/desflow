"""P78: Untrap a waiting entity.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_variable("heard", 0)
    app.add_signal("door", once=False)
    app.add_initial_arrival(
        "w2", size, spawn="keep_listening", attributes={"listening": 1}
    )
    app.add_initial_arrival("w", 1, spawn="watch", attributes={"listening": 1})
    app.add_arrival("d", "constant", value=2)
    app.add_arrival("r", "constant", value=7)

    @app.task(
        name="watch",
        listen={
            "signal": "door",
            "on": [
                {
                    "when": "attr.listening == 1",
                    "path": "P.variables.heard",
                    "value": generator(
                        {"gen": "expr", "expr": "registry['P.variables.heard'] + 1"}
                    ),
                }
            ],
        },
        set={"listening": 0},
    )
    def watch(task_id):
        return {"event_type": "watch"}

    @app.task(
        name="keep_listening",
        listen={
            "signal": "door",
            "on": [
                {
                    "when": "attr.listening == 1",
                    "path": "P.variables.heard",
                    "value": generator(
                        {"gen": "expr", "expr": "registry['P.variables.heard'] + 1"}
                    ),
                }
            ],
        },
    )
    def keep_listening(task_id):
        return {"event_type": "keep_listening"}

    @app.task(name="ring", on=[{"fire": {"signal": "door"}}])
    def ring(task_id):
        return {"event_type": "ring"}

    report_payload = Template(
        {
            "event_type": "report",
            "heard": {"gen": "expr", "expr": "registry['P.variables.heard']"},
        },
        name="report",
    )

    @app.task(name="report")
    def report(task_id):
        return report_payload

    @app.arrival_loop("d")
    def d_arrivals(context):
        for _ in range(3):
            entity = yield context.wait_for_arrival("d")
            context.spawn(ring(context.next_instance("ring"), entity=entity))

    @app.arrival_loop("r")
    def r_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("r")
            context.spawn(report(context.next_instance("report"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
