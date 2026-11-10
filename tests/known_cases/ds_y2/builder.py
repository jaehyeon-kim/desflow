"""DS-Y2: A member picked by a computed rule.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 2) -> SimulationContext:
    app = SimulationContext(sim_id="Y2", factor=0, random_seed=42)
    app.add_resource("desk", family=True, members=3, current_cap=1)
    app.add_resource("b0", current_cap=1)
    app.add_resource("b1", current_cap=1)
    app.add_resource("b2", current_cap=1)
    app.add_resource("pair", family=True, members=2, current_cap=1)
    app.add_arrival(
        "a",
        "constant",
        value=0.5,
        first_at=0,
        attributes={"i": generator({"gen": "sequence", "start": 0})},
        spawn="shortest",
        max_events=5,
    )
    app.add_arrival(
        "b",
        "constant",
        value=0.5,
        first_at=0,
        attributes={"i": generator({"gen": "sequence", "start": 0})},
        spawn="by_index",
        max_events=5,
    )
    app.add_initial_arrival("d", size, spawn="same_step")
    shortest_payload = Template(
        {"event_type": "done", "i": {"gen": "attr", "name": "i"}}, name="tasks.shortest"
    )

    @app.task(
        name="shortest",
        resource_id={"of": "desk", "lowest": "member.queue + member.users"},
        service_time="1 + attr.i",
    )
    def shortest(task_id):
        return shortest_payload

    by_index_payload = Template(
        {"event_type": "done", "i": {"gen": "attr", "name": "i"}}, name="tasks.by_index"
    )

    @app.task(
        name="by_index",
        resource_id={"of": ["b0", "b1", "b2"], "key": "['b0', 'b1', 'b2'][attr.i % 2]"},
        service_time="1 + attr.i",
    )
    def by_index(task_id):
        return by_index_payload

    @app.task(
        name="same_step",
        resource_id={"of": "pair", "lowest": "member.queue + member.users"},
        service_time=1,
    )
    def same_step(task_id):
        return {"event_type": "done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
