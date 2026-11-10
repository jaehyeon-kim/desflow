"""P12: A timeout of 0 or a draw clipped at 0.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_service("zero", dist="constant", value=0)
    app.add_arrival("a", "constant", value=0)
    t_payload = Template(
        {"event_type": "waited", "at": {"gen": "expr", "expr": "t"}}, name="t"
    )

    @app.task(name="t", wait={"timeout": 0}, next="u")
    def t(task_id):
        return t_payload

    u_payload = Template(
        {"event_type": "served", "at": {"gen": "expr", "expr": "t"}}, name="u"
    )

    @app.task(name="u", service_id="zero")
    def u(task_id):
        return u_payload

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(3):
            entity = yield context.wait_for_arrival("a")
            context.spawn(t(context.next_instance("t"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
