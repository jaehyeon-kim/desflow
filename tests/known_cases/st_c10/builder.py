"""ST-C10: Build flat conditions with in-place and.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="iand_and")
    iand_and_payload = Template(
        {
            "event_type": "done",
            "at": {"gen": "expr", "expr": "t"},
            "values": [
                {"gen": "expr", "expr": "wait.t1.value"},
                {"gen": "expr", "expr": "wait.t2.value"},
                {"gen": "expr", "expr": "wait.t0.value"},
            ],
            "in_result": {
                "gen": "expr",
                "expr": "[wait.t1.done, wait.t2.done, wait.t0.done]",
            },
        },
        name="iand_and",
    )

    @app.task(
        name="iand_and",
        wait={
            "all": {
                "t1": {"timeout": {"after": 1, "value": 1}},
                "t2": {"timeout": {"after": 2, "value": 2}},
                "t0": {"timeout": {"after": 0, "value": 0}},
            }
        },
        next="iand_or",
    )
    def iand_and(task_id):
        return iand_and_payload

    iand_or_payload = Template(
        {
            "event_type": "done",
            "at": {"gen": "expr", "expr": "t"},
            "values": [
                {"gen": "expr", "expr": "wait.t1.value"},
                {"gen": "expr", "expr": "wait.t0.value"},
            ],
            "in_result": {
                "gen": "expr",
                "expr": "[wait.t1.done, wait.t2.done, wait.t0.done]",
            },
        },
        name="iand_or",
    )

    @app.task(
        name="iand_or",
        wait={
            "all": {
                "either": {
                    "any": {
                        "t1": {"timeout": {"after": 1, "value": 1}},
                        "t2": {"timeout": {"after": 2, "value": 2}},
                    }
                },
                "t0": {"timeout": {"after": 0, "value": 0}},
            }
        },
        next="ior_or",
    )
    def iand_or(task_id):
        return iand_or_payload

    ior_or_payload = Template(
        {
            "event_type": "done",
            "at": {"gen": "expr", "expr": "t"},
            "values": [{"gen": "expr", "expr": "wait.t0.value"}],
            "in_result": {
                "gen": "expr",
                "expr": "[wait.t1.done, wait.t2.done, wait.t0.done]",
            },
        },
        name="ior_or",
    )

    @app.task(
        name="ior_or",
        wait={
            "any": {
                "either": {
                    "any": {
                        "t1": {"timeout": {"after": 1, "value": 1}},
                        "t2": {"timeout": {"after": 2, "value": 2}},
                    }
                },
                "t0": {"timeout": {"after": 0, "value": 0}},
            }
        },
        next="ior_and",
    )
    def ior_or(task_id):
        return ior_or_payload

    ior_and_payload = Template(
        {
            "event_type": "done",
            "at": {"gen": "expr", "expr": "t"},
            "values": [{"gen": "expr", "expr": "wait.t0.value"}],
            "in_result": {
                "gen": "expr",
                "expr": "[wait.t1.done, wait.t2.done, wait.t0.done]",
            },
        },
        name="ior_and",
    )

    @app.task(
        name="ior_and",
        wait={
            "any": {
                "both": {
                    "all": {
                        "t1": {"timeout": {"after": 1, "value": 1}},
                        "t2": {"timeout": {"after": 2, "value": 2}},
                    }
                },
                "t0": {"timeout": {"after": 0, "value": 0}},
            }
        },
    )
    def ior_and(task_id):
        return ior_and_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
