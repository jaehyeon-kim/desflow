"""EX-2: Carwash where a car keeps its machine during the wash.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 4) -> SimulationContext:
    app = SimulationContext(sim_id="Carwash", factor=0, random_seed=42)
    app.add_resource("machine", current_cap=2)
    app.add_service("wash", dist="constant", value=5)
    app.add_arrival(
        "cars",
        "constant",
        value=7,
        key=lambda n: "Car {}".format(n - 1),
        initial=size,
        spawn="car",
    )

    @app.task(name="car", seize="machine", next="washing")
    def car(task_id):
        return {"event_type": "enters"}

    @app.task(name="washing", wait={"task": "wash"}, release="machine")
    def washing(task_id):
        return {"event_type": "leaves"}

    @app.task(name="wash", service_id="wash")
    def wash(task_id):
        return {"event_type": "washed", "dirt_removed_pct": 70}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=20)
