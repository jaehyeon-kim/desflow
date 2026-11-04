"""DS-V11: A wait group with a predicate of its own.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build() -> SimulationContext:
    app = SimulationContext(sim_id="V11", factor=0, random_seed=42)
    app.add_arrival("a", "constant", value=1, first_at=0)
    gate_payload = Template(
        {"event_type": "opened", "at": {"gen": "expr", "expr": "t"}}, name="gate"
    )

    @app.task(
        name="gate",
        wait={
            "when": "done_count == 1 and t < 5",
            "of": {"a": {"timeout": 1}, "b": {"timeout": 3}},
        },
    )
    def gate(task_id):
        return gate_payload

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("a")
            context.spawn(gate(context.next_instance("gate"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
