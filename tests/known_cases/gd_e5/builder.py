"""GD-E5: Read which process is running.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="E", factor=0, random_seed=42)
    app.add_initial_arrival("p1", size, spawn="my_proc")
    my_proc_payload = Template(
        {"event_type": "seen", "me": {"gen": "expr", "expr": "process.name"}},
        name="my_proc",
    )

    @app.task(name="my_proc", next="sub")
    def my_proc(task_id):
        return my_proc_payload

    sub_payload = Template(
        {
            "event_type": "seen_in_sub",
            "me": {"gen": "expr", "expr": "process.flow_name"},
        },
        name="sub",
    )

    @app.task(name="sub")
    def sub(task_id):
        return sub_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
