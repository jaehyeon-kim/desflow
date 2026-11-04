"""GA-5: One option gets 60 percent of votes.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Vote", factor=0, random_seed=42)
    app.add_arrival("votes", "constant", value=1, spawn="vote")
    vote_payload = Template(
        {"choice": {"gen": "weighted", "options": {"a": 0.6, "b": 0.4}}},
        name="tasks.vote",
    )

    @app.task(name="vote")
    def vote(task_id):
        return vote_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=10000)
