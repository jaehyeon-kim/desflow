"""ST-C06: A failing event inside a nested condition reaches the waiter.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="stage_1")

    @app.task(
        name="stage_1",
        wait={"all": {"x": {"task": "explode_1"}, "t": {"timeout": 1}}},
        on_fail="caught_1",
    )
    def stage_1(task_id):
        return {"event_type": "unreachable"}

    caught_1_payload = Template(
        {"event_type": "caught", "reason": {"gen": "expr", "expr": "upstream.reason"}},
        name="caught_1",
    )

    @app.task(name="caught_1", next="stage_2")
    def caught_1(task_id):
        return caught_1_payload

    @app.task(name="explode_1", service_time=1, fail="True", fail_reason="Onoes!")
    def explode_1(task_id):
        return {"event_type": "child"}

    @app.task(
        name="stage_2",
        wait={"any": {"x": {"task": "explode_2"}, "t": {"timeout": 1}}},
        on_fail="caught_2",
    )
    def stage_2(task_id):
        return {"event_type": "unreachable"}

    caught_2_payload = Template(
        {"event_type": "caught", "reason": {"gen": "expr", "expr": "upstream.reason"}},
        name="caught_2",
    )

    @app.task(name="caught_2", next="stage_3")
    def caught_2(task_id):
        return caught_2_payload

    @app.task(
        name="explode_2",
        service_time=0,
        fail="True",
        fail_reason="Onoes, failed after 0!",
    )
    def explode_2(task_id):
        return {"event_type": "child"}

    @app.task(
        name="stage_3",
        wait={
            "any": {
                "both": {"all": {"x": {"task": "explode_3"}, "t": {"timeout": 1}}},
                "t2": {"timeout": 1},
            }
        },
        on_fail="caught_3",
    )
    def stage_3(task_id):
        return {"event_type": "unreachable"}

    caught_3_payload = Template(
        {"event_type": "caught", "reason": {"gen": "expr", "expr": "upstream.reason"}},
        name="caught_3",
    )

    @app.task(name="caught_3")
    def caught_3(task_id):
        return caught_3_payload

    @app.task(
        name="explode_3",
        service_time=0,
        fail="True",
        fail_reason="Onoes, failed after 0!",
    )
    def explode_3(task_id):
        return {"event_type": "child"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
