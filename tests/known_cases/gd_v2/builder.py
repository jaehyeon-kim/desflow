"""GD-V2: Succeed an event, fail an event and trigger one from another.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="V", factor=0, random_seed=42)
    app.add_signal("a", once=True)
    app.add_signal("b", once=True)
    app.add_signal("c", once=True)
    app.add_initial_arrival("x", size, spawn="ok_waiter")
    app.add_initial_arrival("y", 1, spawn="fail_waiter")
    app.add_initial_arrival("z", 1, spawn="driver")
    ok_waiter_payload = Template(
        {"event_type": "ok", "v": {"gen": "expr", "expr": "wait.a.value"}},
        name="ok_waiter",
    )

    @app.task(name="ok_waiter", wait={"signal": "a"})
    def ok_waiter(task_id):
        return ok_waiter_payload

    @app.task(name="fail_waiter", wait={"signal": "b"}, on_fail="caught")
    def fail_waiter(task_id):
        return {"event_type": "fine"}

    caught_payload = Template(
        {"event_type": "caught", "why": {"gen": "expr", "expr": "upstream.reason"}},
        name="caught",
    )

    @app.task(name="caught")
    def caught(task_id):
        return caught_payload

    @app.task(
        name="driver",
        service_time=1,
        on=[
            {"fire": {"signal": "a", "value": 7}},
            {"fire": {"signal": "b", "fail": True, "value": "oops", "defused": False}},
            {"fire": {"signal": "c", "from": "a"}},
        ],
    )
    def driver(task_id):
        return {"event_type": "drive"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
