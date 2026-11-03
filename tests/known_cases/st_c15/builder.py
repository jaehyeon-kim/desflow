"""ST-C15: Two processes wait on the same and condition.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("p1", size, spawn="and_1")
    app.add_initial_arrival("p2", 1, spawn="and_2")
    app.add_initial_arrival("p3", 1, spawn="or_1")
    app.add_initial_arrival("p4", 1, spawn="or_2")
    and_1_payload = Template(
        {
            "event_type": "done",
            "at": {"gen": "expr", "expr": "t"},
            "values": [
                {"gen": "expr", "expr": "wait.t0.value"},
                {"gen": "expr", "expr": "wait.t1.value"},
            ],
            "in_result": {"gen": "expr", "expr": "[wait.t0.done, wait.t1.done]"},
        },
        name="and_1",
    )

    @app.task(
        name="and_1",
        wait={
            "all": {
                "t0": {"timeout": {"after": 0, "value": 0}},
                "t1": {"timeout": {"after": 1, "value": 1}},
            }
        },
    )
    def and_1(task_id):
        return and_1_payload

    and_2_payload = Template(
        {
            "event_type": "done",
            "at": {"gen": "expr", "expr": "t"},
            "values": [
                {"gen": "expr", "expr": "wait.t0.value"},
                {"gen": "expr", "expr": "wait.t1.value"},
                {"gen": "expr", "expr": "wait.t2.value"},
            ],
            "in_result": {
                "gen": "expr",
                "expr": "[wait.t0.done, wait.t1.done, wait.t2.done]",
            },
        },
        name="and_2",
    )

    @app.task(
        name="and_2",
        wait={
            "all": {
                "both": {
                    "all": {
                        "t0": {"timeout": {"after": 0, "value": 0}},
                        "t1": {"timeout": {"after": 1, "value": 1}},
                    }
                },
                "t2": {"timeout": {"after": 2, "value": 2}},
            }
        },
    )
    def and_2(task_id):
        return and_2_payload

    or_1_payload = Template(
        {
            "event_type": "done",
            "at": {"gen": "expr", "expr": "t"},
            "values": [{"gen": "expr", "expr": "wait.t0.value"}],
            "in_result": {"gen": "expr", "expr": "[wait.t0.done, wait.t1.done]"},
        },
        name="or_1",
    )

    @app.task(
        name="or_1",
        wait={
            "any": {
                "t0": {"timeout": {"after": 0, "value": 0}},
                "t1": {"timeout": {"after": 1, "value": 1}},
            }
        },
    )
    def or_1(task_id):
        return or_1_payload

    or_2_payload = Template(
        {
            "event_type": "done",
            "at": {"gen": "expr", "expr": "t"},
            "values": [{"gen": "expr", "expr": "wait.t0.value"}],
            "in_result": {
                "gen": "expr",
                "expr": "[wait.t0.done, wait.t1.done, wait.t2.done]",
            },
        },
        name="or_2",
    )

    @app.task(
        name="or_2",
        wait={
            "any": {
                "either": {
                    "any": {
                        "t0": {"timeout": {"after": 0, "value": 0}},
                        "t1": {"timeout": {"after": 1, "value": 1}},
                    }
                },
                "t2": {"timeout": {"after": 2, "value": 2}},
            }
        },
    )
    def or_2(task_id):
        return or_2_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
