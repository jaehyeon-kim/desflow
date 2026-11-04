"""DX-2: A lane that gives back the newest truck.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Dock", factor=0, random_seed=42)
    app.add_store("lane", current_cap=2, max_cap=2, priority=True)
    app.add_container("tank", current_cap=100, max_cap=100)
    app.add_arrival(
        "diesel_trucks",
        "constant",
        value=3,
        attributes={"fuel": "diesel"},
        spawn="park",
    )
    app.add_arrival(
        "petrol_trucks",
        "constant",
        value=5,
        attributes={"fuel": "petrol"},
        spawn="park",
    )
    app.add_arrival("crew", "constant", value=7, spawn="unload")
    app.add_arrival("tankers", "constant", value=4, spawn="pump")
    app.add_arrival("marshals", "constant", value=15, spawn="marshal")

    @app.task(name="park", put={"store": "lane", "priority": "-t"})
    def park(task_id, entity):
        return {"event_type": "truck_parked", "fuel": entity.attributes.get("fuel")}

    @app.task(name="unload", get={"store": "lane"})
    def unload(task_id, item):
        return {"event_type": "truck_unloaded", "truck": item}

    @app.task(name="pump", put={"container": "tank", "amount": 40, "wait": False})
    def pump(task_id):
        return {"event_type": "pump_started"}

    @app.task(name="marshal", wait={"condition": "putters.lane >= 1"})
    def marshal(task_id):
        return {"event_type": "overflow_bay_called"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=40)
