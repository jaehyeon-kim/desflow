"""DS-M3: Handle a failed wait with on_fail.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="M3", factor=0, random_seed=42)
    app.add_signal("may_fail", once=True)
    app.add_initial_arrival("a", size, spawn="t")
    app.add_arrival("b", "constant", value=1, spawn="breaker", max_events=1)

    @app.task(name="t", wait={"signal": "may_fail"}, on_fail="handle")
    def t(task_id):
        return {"event_type": "fine"}

    handle_payload = Template(
        {"event_type": "handled", "reason": {"gen": "expr", "expr": "upstream.reason"}},
        name="tasks.handle",
    )

    @app.task(name="handle")
    def handle(task_id):
        return handle_payload

    @app.task(
        name="breaker",
        on=[{"fire": {"signal": "may_fail", "fail": True, "value": "event"}}],
    )
    def breaker(task_id):
        return {"event_type": "break"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
