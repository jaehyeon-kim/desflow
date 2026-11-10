"""GD-V7: Or, and, nested conditions and timeout values.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="V", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="any_stage")
    any_stage_payload = Template(
        {
            "event_type": "any_done",
            "first": {"gen": "expr", "expr": "wait.t1.value"},
            "second_missing": {"gen": "expr", "expr": "not wait.t2.done"},
        },
        name="any_stage",
    )

    @app.task(
        name="any_stage",
        wait={
            "any": {
                "t1": {"timeout": {"after": 1, "value": "spam"}},
                "t2": {"timeout": {"after": 2, "value": "eggs"}},
            }
        },
        next="all_stage",
    )
    def any_stage(task_id):
        return any_stage_payload

    all_stage_payload = Template(
        {
            "event_type": "all_done",
            "values": [
                {"gen": "expr", "expr": "wait.t1.value"},
                {"gen": "expr", "expr": "wait.t2.value"},
            ],
        },
        name="all_stage",
    )

    @app.task(
        name="all_stage",
        wait={
            "all": {
                "t1": {"timeout": {"after": 1, "value": "spam"}},
                "t2": {"timeout": {"after": 2, "value": "eggs"}},
            }
        },
        next="nest_stage",
    )
    def all_stage(task_id):
        return all_stage_payload

    nest_stage_payload = Template(
        {
            "event_type": "nest_done",
            "all_processed": {
                "gen": "expr",
                "expr": "wait.e1.done and wait.e2.done and wait.e3.done",
            },
        },
        name="nest_stage",
    )

    @app.task(
        name="nest_stage",
        wait={
            "all": {
                "left": {"any": {"e1": {"timeout": 0}, "e2": {"timeout": 1}}},
                "e3": {"timeout": 2},
            }
        },
    )
    def nest_stage(task_id):
        return nest_stage_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
