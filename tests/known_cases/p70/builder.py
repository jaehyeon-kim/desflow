"""P70: Log a computed message.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_arrival("a", "constant", value=1, key=lambda n: str(n - 1))
    t_payload = Template(
        {
            "event_type": "log",
            "message": {
                "gen": "format",
                "template": "entity {k} at {now:g}",
                "names": {"k": {"gen": "entity"}, "now": {"gen": "expr", "expr": "t"}},
            },
        },
        name="t",
    )

    @app.task(name="t")
    def t(task_id):
        return t_payload

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(3):
            entity = yield context.wait_for_arrival("a")
            context.spawn(t(context.next_instance("t"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
