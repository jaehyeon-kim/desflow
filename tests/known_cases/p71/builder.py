"""P71: Event times of 0.00001 and 0.00002 stay different.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="t")
    t_payload = Template(
        {"event_type": "first", "at": {"gen": "expr", "expr": "t"}}, name="t"
    )

    @app.task(name="t", service_time=1e-05, next="u")
    def t(task_id):
        return t_payload

    u_payload = Template(
        {"event_type": "second", "at": {"gen": "expr", "expr": "t"}}, name="u"
    )

    @app.task(name="u", service_time=1e-05)
    def u(task_id):
        return u_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
