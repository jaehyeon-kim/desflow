"""DS-Y3: A wait on a computed number of events.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="Y3", factor=0, random_seed=42)
    app.add_store("pipe", family=True, members=3)
    app.add_initial_arrival(
        "a", size, spawn="fan", attributes={"n": 3, "delays": [4, 1, 2]}
    )
    fan_payload = Template(
        {
            "event_type": "all_done",
            "values": {"gen": "expr", "expr": "wait.each.values"},
        },
        name="fan",
    )

    @app.task(
        name="fan",
        wait={
            "each": "attr.n",
            "group": "all",
            "branch": {
                "task": "worker",
                "args": {"i": generator({"gen": "expr", "expr": "each.index"})},
            },
        },
        next="first",
    )
    def fan(task_id):
        return fan_payload

    @app.task(
        name="first",
        wait={
            "each": "attr.delays",
            "group": "any",
            "branch": {"timeout": "each.value"},
        },
        next="broadcast",
    )
    def first(task_id):
        return {"event_type": "first_done"}

    @app.task(
        name="broadcast",
        wait={
            "each": "members(pipe)",
            "branch": {
                "put": {"store": {"of": "pipe", "key": "each.value"}, "item": 5}
            },
        },
    )
    def broadcast(task_id):
        return {"event_type": "sent"}

    worker_payload = Template(
        {"event_type": "worker", "v": {"gen": "expr", "expr": "arg.i * 10"}},
        name="worker",
    )

    @app.task(name="worker", service_time="arg.i + 1")
    def worker(task_id):
        return worker_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
