"""ST-C17: The values of a condition.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="value")
    value_payload = Template(
        {
            "event_type": "done",
            "at": {"gen": "expr", "expr": "t"},
            "values": [
                {"gen": "expr", "expr": "wait.t0.value"},
                {"gen": "expr", "expr": "wait.t1.value"},
                {"gen": "expr", "expr": "wait.t2.value"},
            ],
            "has_first": {"gen": "expr", "expr": "wait.t0.done"},
            "first": {"gen": "expr", "expr": "wait.t0.value"},
        },
        name="value",
    )

    @app.task(
        name="value",
        wait={
            "all": {
                "t0": {"timeout": {"after": 0, "value": 0}},
                "t1": {"timeout": {"after": 1, "value": 1}},
                "t2": {"timeout": {"after": 2, "value": 2}},
            }
        },
        next="result_order",
    )
    def value(task_id):
        return value_payload

    result_order_payload = Template(
        {
            "event_type": "done",
            "order": {"gen": "expr", "expr": "[name for name in wait.grp.value]"},
        },
        name="result_order",
    )

    @app.task(
        name="result_order",
        wait={
            "all": {
                "grp": {
                    "all": {
                        "t2": {"timeout": 2},
                        "t1": {"timeout": 1},
                        "t0": {"timeout": 0},
                    }
                }
            }
        },
        next="nested_result_order",
    )
    def result_order(task_id):
        return result_order_payload

    nested_result_order_payload = Template(
        {
            "event_type": "done",
            "order": {"gen": "expr", "expr": "[name for name in wait.grp.value]"},
        },
        name="nested_result_order",
    )

    @app.task(
        name="nested_result_order",
        wait={
            "all": {
                "grp": {
                    "all": {
                        "either": {"any": {"t0": {"timeout": 0}, "t1": {"timeout": 1}}},
                        "t2": {"timeout": 2},
                    }
                }
            }
        },
    )
    def nested_result_order(task_id):
        return nested_result_order_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
