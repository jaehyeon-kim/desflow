"""CV-126: A computed number of members made during the run.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 3) -> SimulationContext:
    app = SimulationContext(sim_id="S", factor=0, random_seed=42)
    app.add_resource("desk", family=True, current_cap="attr.i + 1")
    app.add_initial_arrival(
        "a",
        size,
        spawn="job",
        attributes={"i": generator({"gen": "sequence", "start": 0})},
    )
    job_payload = Template(
        {
            "e": "done",
            "x": {
                "gen": "expr",
                "expr": "[users.desk[attr.i % 2], queue.desk[attr.i % 2], "
                "users.desk[7], members(desk)]",
            },
        },
        name="job",
    )

    @app.task(
        name="job", resource_id={"of": "desk", "key": "attr.i % 2"}, service_time=2
    )
    def job(task_id):
        return job_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
