"""P36: A priority from an attribute that a step changes.

Builder form of `model.yaml`.
"""

from pathlib import Path

from desflow import NO_RECORD, SimulationContext

HERE = Path(__file__).parent


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1, queue="priority")
    app.add_service("s", dist="constant", value=1)
    app.add_arrival_replay(
        "o", HERE / "p36.csv", time="time", unit="s", fields=["i", "pr"], spawn="t"
    )

    @app.task(
        name="t",
        next={
            "when": [{"condition": "attr.i % 4 == 0", "task": "setp"}],
            "default": "u",
        },
    )
    def t(task_id):
        return NO_RECORD

    @app.task(name="setp", set={"pr": 0}, next="u")
    def setp(task_id):
        return NO_RECORD

    @app.task(name="u", seize={"resource": "m", "priority": "attr.pr"}, next="hold")
    def u(task_id):
        return {"event_type": "started"}

    @app.task(name="hold", service_id="s", release="m")
    def hold(task_id):
        return NO_RECORD

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
